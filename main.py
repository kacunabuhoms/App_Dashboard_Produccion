import streamlit as st
import gspread
import pandas as pd
import pytz
import requests
import json
import io
import numpy as np
from gspread_dataframe import set_with_dataframe
from datetime import datetime, timedelta, timezone
from PIL import Image
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

st.set_page_config(layout="wide")


def fetch_monday_initial_data(board_ids, group_ids, api_key):
    api_url = 'https://api.monday.com/v2'
    headers = {'Authorization': api_key, 'Content-Type': 'application/json'}
    
    # Convertimos las listas de IDs en cadenas de texto adecuadas para la consulta
    board_ids_str = ','.join(map(str, board_ids))  # Convierte a string y une con comas
    group_ids_str = ','.join([f'"{id}"' for id in group_ids])  # Encierra cada ID en comillas y une con comas
    
    # Insertamos las variables directamente en la consulta
    query = f"""
    {{
      boards(ids: [{board_ids_str}]) {{
        groups (ids: [{group_ids_str}]) {{
          items_page (limit:500){{
            cursor
            items {{
              id
              name
              column_values {{
                id
                text
                column {{
                  title
                }}
              }}
            }}
          }}
        }}
      }}
    }}     
    """
    
    # Envía la solicitud a monday.com
    response = requests.post(api_url, json={'query': query}, headers=headers)
    data = response.json()
    
    # Obtener el cursor si está disponible
    cursor = data.get('data', {}).get('boards', [{}])[0].get('groups', [{}])[0].get('items_page', {}).get('cursor')
    
    # Imprime la respuesta para depuración
    # print(json.dumps(data, indent=2))
    print(f"Cursor inicial: {cursor}")
    print("JSON from initial query has been saved")

    return data, cursor


def fetch_next_items_page(cursor, api_key):
    api_url = 'https://api.monday.com/v2'
    headers = {'Authorization': api_key, 'Content-Type': 'application/json'}
    
    query = f"""
    {{
      next_items_page (limit:500, cursor: "{cursor}") {{
        cursor
        items {{
          id
          name
          column_values {{
            id
            text
            column {{
              title
            }}
          }}
        }}
      }}
    }}
    """
    
    response = requests.post(api_url, json={'query': query}, headers=headers)
    data = response.json()
    
    new_cursor = data.get('data', {}).get('next_items_page', {}).get('cursor')
    
    return data, new_cursor


def run_queries_until_complete(initial_cursor, api_key):
    current_cursor = initial_cursor
    json_queries = {}
    query_count = 1  # Para llevar la cuenta de las consultas
    
    while current_cursor:
        data, current_cursor = fetch_next_items_page(current_cursor, api_key)
        
        if data.get('data', {}).get('next_items_page', {}).get('items', []):
            json_queries[f'query{query_count}'] = data  # Almacenar directamente el JSON
            print(f"JSON from query {query_count} has been saved.")
        else:
            print("No data received.")
        
        query_count += 1
        
        # Si no hay nuevo cursor, imprimir que no hay más datos y romper el ciclo
        if not current_cursor:
            print("No more data available.")
            break

    return json_queries


def create_dataframe_from_json_initial(json_data, column_ids):
    # Extraer los datos de los ítems desde el JSON
    items_data = json_data['data']['boards'][0]['groups'][0]['items_page']['items']
    data_list = []
    
    for item in items_data:
        # Incluir el ID del ítem en la información
        item_info = {
            'Item ID': item['id'],  # Guarda el ID del ítem
            'Item Name': item['name']
        }
        
        for column_value in item['column_values']:
            if column_value['id'] in column_ids:
                item_info[column_value['column']['title']] = column_value['text']
        
        data_list.append(item_info)

    return pd.DataFrame(data_list)


def create_dataframe_from_multiple_queries(json_data, column_ids):
    all_data = []  # Lista para almacenar todos los datos de cada query

    # Iterar sobre cada query en el diccionario
    for query_key, query_value in json_data.items():
        items_data = query_value['data']['next_items_page']['items']
        query_data_list = []

        for item in items_data:
            item_info = {
            'Item ID': item['id'],  # Guarda el ID del ítem
            'Item Name': item['name']
            }

            # Mapeo de id de columna a texto para facilitar el acceso
            column_value_map = {cv['id']: cv['text'] for cv in item['column_values'] if 'text' in cv}

            # Extraer las columnas deseadas basadas en los IDs proporcionados
            for col_id in column_ids:
                column_title = next((cv['column']['title'] for cv in item['column_values'] if cv['id'] == col_id), col_id)
                item_info[column_title] = column_value_map.get(col_id, '')  # Usar cadena vacía si no hay valor
            
            query_data_list.append(item_info)

        all_data.extend(query_data_list)  # Añadir los datos de esta query a la lista general



    

    # Crear un DataFrame con todos los datos recopilados de cada query
    return pd.DataFrame(all_data)


def fetch_full_data():
    initial_query, initial_cursor = fetch_monday_initial_data(board_ids, group_ids, api_key)
    df_produccion = create_dataframe_from_json_initial(initial_query, column_ids)
    print("Dataframe inicial creado")
    
    if len(df_produccion) > 499:
        json_data = run_queries_until_complete(initial_cursor, api_key)
        df_pages = create_dataframe_from_multiple_queries(json_data ,column_ids)
        df_produccion = pd.concat([df_produccion, df_pages], ignore_index=True)
        print("---------- Dataframe completo creado ----------")
    else:
        df_return = df_produccion

    columnas_a_eliminar = [
    "Fecha de Preproyecto", "Fecha de ODC", "Fecha de Preprensa", 
    "Fecha de Impresión", "Fecha de Acabados", "Fecha de Producto Terminado",
    "Fecha de Recibido en Planta", "Fecha de Logistica", "Fecha ODT Completa", "Fillrate"
    ]
    
    df_return = df_produccion.drop(columns=columnas_a_eliminar)
    df_return = df_return[df_return['Fecha Inicio ODT'].notna() & (df_return['Fecha Inicio ODT'] != "")]
    return df_return


def fetch_activity_logs(api_key, board_id, from_date, to_date):
    api_url = 'https://api.monday.com/v2'
    headers = {'Authorization': api_key, 'Content-Type': 'application/json'}
    
    # Formatear la fecha de "to:" para incluirla en el query
    formatted_to_date = to_date.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    query = f"""
      {{
        boards (ids: {board_id}) {{
          activity_logs (limit:10000, from: "{from_date}T00:00:00Z", to: "{formatted_to_date}") {{
            created_at
            id
            event
            data
          }}
        }}
      }}
    """
    
    response = requests.post(api_url, json={'query': query}, headers=headers)
    data = response.json()

    activity_logs = data['data']['boards'][0]['activity_logs']
    rows = []
    for log in activity_logs:
        log_data = json.loads(log['data'])
        created_at = int(log['created_at']) / 10000000  # Get timestamp in seconds
        # Correct for 6-hour shift by subtracting 6 hours
        created_at = datetime.fromtimestamp(created_at, timezone.utc) - timedelta(hours=6)
        created_at_str = created_at.strftime('%Y-%m-%d %H:%M:%S')  # Convert to string in the desired format
        rows.append({
            'id': log['id'],
            'created_at': created_at_str,
            'group_id': log_data.get('group_id', ''),
            'pulse_name': log_data.get('pulse_name', ''),
            'pulse_id': log_data.get('pulse_id',''),
            'column_title': log_data.get('column_title', ''),
            'text': log_data.get('value', {}).get('label', {}).get('text', '') if log_data.get('value') else ''
        })
    
    df = pd.DataFrame(rows)
    print("JSON Query extraido")
    
    # Check if 10,000 records were retrieved, call function recursively if true
    if len(df) == 10000:
        last_date = datetime.strptime(df.iloc[-1]['created_at'], '%Y-%m-%d %H:%M:%S') + timedelta(days=1)
        df = pd.concat([df, fetch_activity_logs(api_key, board_id, from_date, last_date)], ignore_index=True)

    # Drop duplicates based on 'id'
    df = df.drop_duplicates(subset=['id'], keep='first')

    # Convert 'created_at' to datetime for sorting
    df['created_at'] = pd.to_datetime(df['created_at'])

    # Sort DataFrame in descending order by 'created_at'
    df = df.sort_values(by='created_at', ascending=False)

    # Convert 'created_at' to string in the desired format
    df['created_at'] = df['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    return df


def dataframes_cross_full(df_produccion, df_activity_logs):
    df_activity_logs_filtered = df_activity_logs[df_activity_logs['text'] == 'Done'].copy()

    df_activity_logs_filtered['column_title'] = 'Fecha final ' + df_activity_logs_filtered['column_title']
    
    valores_interes = [
        'Fecha final Preprensa', 'Fecha final Preproyecto', 'Fecha final ODC', 
        'Fecha final Acabados', 'Fecha final Impresión', 'Fecha final ODT Completo', 
        'Fecha final Logistica', 'Fecha final Recibido en Planta', 'Fecha final Recibido en planta'
    ]

    df_activity_logs_filtered = df_activity_logs_filtered[df_activity_logs_filtered['column_title'].isin(valores_interes)]

    df_produccion['Item ID'] = df_produccion['Item ID'].astype(str)

    df_activity_logs_filtered['Item ID'] = df_activity_logs_filtered['Item ID'].astype(str)

    df_pivoted = df_activity_logs_filtered.pivot_table(
        index='Item ID',
        columns='column_title',
        values='created_at',
        aggfunc='first'  # Usa 'first' en caso de múltiples entradas
    ).reset_index()
    
    df_resultado = df_produccion.merge(df_pivoted, on='Item ID', how='left')

    df_resultado['Fecha final Recibido en Planta'] = df_resultado['Fecha final Recibido en Planta'].combine_first(
                                                     df_resultado['Fecha final Recibido en planta'])
    
    df_resultado = df_resultado.drop(columns=['Fecha final Recibido en planta'])

    nuevo_orden = [
        'Item ID', 'Item Name', 'Descripción', 'Fecha Inicio ODT', 'Fecha Final ODT', 
        'Fecha fin ODC', 'Cliente', 'Planta', 
        'Preproyecto', 'ODC', 'Preprensa', 
        'Impresión', 'Acabados', 'Logistica', 
        'ODT Completo', 
        'Fecha final Preproyecto', 'Fecha final ODC', 'Fecha final Preprensa', 
        'Fecha final Impresión', 'Fecha final Acabados', 'Fecha final Logistica', 
        'Fecha final ODT Completo', 'Fecha final Recibido en Planta'
    ]

    # Reordenar el DataFrame
    df_resultado = df_resultado[nuevo_orden]

    df_resultado['Fecha final Preproyecto'] = pd.to_datetime(df_resultado['Fecha final Preproyecto'])
    df_resultado['Fecha final ODC'] = pd.to_datetime(df_resultado['Fecha final ODC'])

    return df_resultado


def dataframes_cross_active(df_produccion, df_activity_logs):
    df_activity_logs_filtered = df_activity_logs[df_activity_logs['text'] == 'Done'].copy()

    df_activity_logs_filtered['column_title'] = 'Fecha final ' + df_activity_logs_filtered['column_title']
    
    valores_interes = [
        'Fecha final Preprensa', 'Fecha final Preproyecto', 'Fecha final ODC', 
        'Fecha final Acabados', 'Fecha final Impresión', 'Fecha final ODT Completo', 
        'Fecha final Logistica', 'Fecha final Recibido en Planta'
    ]

    df_activity_logs_filtered = df_activity_logs_filtered[df_activity_logs_filtered['column_title'].isin(valores_interes)]

    df_produccion['Item ID'] = df_produccion['Item ID'].astype(str)

    df_activity_logs_filtered['Item ID'] = df_activity_logs_filtered['Item ID'].astype(str)

    df_pivoted = df_activity_logs_filtered.pivot_table(
        index='Item ID',
        columns='column_title',
        values='created_at',
        aggfunc='first'  # Usa 'first' en caso de múltiples entradas
    ).reset_index()
    
    df_resultado = df_produccion.merge(df_pivoted, on='Item ID', how='left')

    nuevo_orden = [
        'Item ID', 'Item Name', 'Descripción', 'Fecha Inicio ODT', 'Fecha Final ODT', 
        'Fecha fin ODC', 'Cliente', 'Planta', 
        'Preproyecto', 'ODC', 'Preprensa', 
        'Impresión', 'Acabados', 'Logistica', 
        'ODT Completo', 
        'Fecha final Preproyecto', 'Fecha final ODC', 'Fecha final Preprensa', 
        'Fecha final Impresión', 'Fecha final Acabados', 'Fecha final Logistica', 
        'Fecha final ODT Completo', 'Fecha final Recibido en Planta'
    ]

    # Reordenar el DataFrame
    df_resultado = df_resultado[nuevo_orden]

    df_resultado['Fecha final Preproyecto'] = pd.to_datetime(df_resultado['Fecha final Preproyecto'])
    df_resultado['Fecha final ODC'] = pd.to_datetime(df_resultado['Fecha final ODC'])

    return df_resultado


def task_time(df_final):
    # Lista de columnas de fechas en orden
    date_columns = [
        'Fecha Inicio ODT',
        'Fecha final Preproyecto',
        'Fecha final ODC',
        'Fecha final Preprensa',
        'Fecha final Impresión',
        'Fecha final Acabados',
        'Fecha final Logistica',
        'Fecha final ODT Completo'
    ]

    # Convertir todas las columnas de fechas a datetime
    for col in date_columns:
        df_final[col] = pd.to_datetime(df_final[col])

    # Iterar sobre las columnas de fecha a partir de la segunda
    for i in range(1, len(date_columns)):
        current_col = date_columns[i]
        duration_col = 'Duración ' + current_col.replace('Fecha final ', '')
        
        # Función para obtener la fecha anterior válida a la izquierda
        def get_previous_date(row):
            left_dates = row[date_columns[:i]]
            non_null_dates = left_dates[left_dates.notnull()]
            if len(non_null_dates) == 0:
                return np.nan
            else:
                return non_null_dates.iloc[-1]
        
        # Aplicar la función a cada fila
        previous_dates = df_final.apply(get_previous_date, axis=1)
        
        # Calcular la duración solo si hay fecha actual y fecha anterior
        mask = df_final[current_col].notnull() & previous_dates.notnull()
        df_final[duration_col] = np.nan
        
        # Calcular los días hábiles entre las dos fechas
        # Utiliza numpy.busday_count para contar los días hábiles
        df_final.loc[mask, duration_col] = df_final.loc[mask].apply(
            lambda row: np.busday_count(previous_dates.loc[row.name].date(), row[current_col].date()),
            axis=1
        )

        # **Eliminamos cualquier ajuste basado en horas, ya que solo se trabaja con las fechas**
    
    return df_final


def task_time_with_hours(df_final):
    # Lista de columnas de fechas en orden
    date_columns = [
        'Fecha Inicio ODT',
        'Fecha final Preproyecto',
        'Fecha final ODC',
        'Fecha final Preprensa',
        'Fecha final Impresión',
        'Fecha final Acabados',
        'Fecha final Logistica',
        'Fecha final ODT Completo'
    ]

    # Convertir todas las columnas de fechas a datetime
    for col in date_columns:
        df_final[col] = pd.to_datetime(df_final[col])

    # Iterar sobre las columnas de fecha a partir de la segunda
    for i in range(1, len(date_columns)):
        current_col = date_columns[i]
        duration_col = 'Duración ' + current_col.replace('Fecha final ', '')
        
        # Función para obtener la fecha anterior válida a la izquierda
        def get_previous_date(row):
            left_dates = row[date_columns[:i]]
            non_null_dates = left_dates[left_dates.notnull()]
            if len(non_null_dates) == 0:
                return np.nan
            else:
                return non_null_dates.iloc[-1]
        
        # Aplicar la función a cada fila
        previous_dates = df_final.apply(get_previous_date, axis=1)
        
        # Calcular la duración solo si hay fecha actual y fecha anterior
        mask = df_final[current_col].notnull() & previous_dates.notnull()
        df_final[duration_col] = np.nan
        
        # Calcular los días hábiles entre las dos fechas
        # Utiliza numpy.busday_count para contar los días hábiles
        df_final.loc[mask, duration_col] = df_final.loc[mask].apply(
            lambda row: np.busday_count(previous_dates.loc[row.name].date(), row[current_col].date()),
            axis=1
        )

        # Ajuste para incluir las horas si es necesario
        time_diff = df_final.loc[mask, current_col] - previous_dates.loc[mask]
        seconds_part = time_diff.dt.seconds
        
        # Determinar si hay más de 12 horas (43200 segundos)
        hours = 18
        adjustment = (seconds_part > (hours * 3600)).astype(int)
        
        # Aplicar el ajuste
        df_final.loc[mask, duration_col] += adjustment
    
    return df_final


def clean_data(df):
    # Lista de columnas de duración que deseas verificar
    duration_columns = [
        'Duración Preproyecto', 'Duración ODC', 'Duración Preprensa',
        'Duración Impresión', 'Duración Acabados', 'Duración Logistica',
        'Duración ODT Completo'
    ]
    
    # Eliminar filas donde la columna 'Cliente' es nula
    df = df[df['Cliente'].notnull()]
    
    # Filtrar el DataFrame para eliminar filas donde cualquier columna de duración tenga un valor menor a 0
    for column in duration_columns:
        df = df[df[column] >= 0]
    
    return df


#------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------

# Personalización de la página
tz = pytz.timezone('America/Monterrey')

# Configuración de gspread para conectar con Google Sheets
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"],
    scopes=SCOPES
)
# Usar las credenciales para autenticarse con Google Sheets
gc = gspread.authorize(credentials)
sh = gc.open_by_key('1pvFWzdLybMmK6RT_yPauYytbvzBhVFT2nOcbJ91XQaI')

# Construye el servicio de la API de Google Drive
service = build('drive', 'v3', credentials=credentials)

# ID del archivo en Google Drive que deseas descargar
file_id = '1xIIzJsNCfuTpxAgXehy2r7QVEIsnl7Ks'
request = service.files().get_media(fileId=file_id)
fh = io.BytesIO()
downloader = MediaIoBaseDownload(fh, request)
done = False
while done is False:
    status, done = downloader.next_chunk()

# Utiliza PIL para abrir la imagen desde el stream de bytes
fh.seek(0)
image = Image.open(fh)

# Mostrar la imagen en Streamlit
st.logo(image)





#------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------

# Tab para Proyectos Activos
tab_activo, tab_cerrado = st.tabs(["Proyectos Activos", "Proyectos Cerrados"])
with tab_activo:
    if st.button("DF", key="Activos"):
            board_ids = ["2354185091"]  # Ejemplo de lista de IDs de tableros
            group_ids = ["topics"]  # Ejemplo de lista de IDs de grupos: "new_group11120"
            api_key = 'eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjQwNTEwNjk2OCwiYWFpIjoxMSwidWlkIjo2Mzg5NDk0MCwiaWFkIjoiMjAyNC0wOS0wMlQxODoyNjo0OS4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MTk3MDA2NSwicmduIjoidXNlMSJ9.XSia7vseMdnGXBQ2PiCjYNUtch-bOxeXQZeXv_8q1iI'  # Reemplaza esto con tu token de API real
            column_ids = [
                "text8", "date5", "date20", "fecha", "dropdown6", "label", "dup__of_status_17", "status_1",
                "dup__of_status_10", "dup__of_status_11", "dup__of_status_19", "dup__of_empaque", "status_14",
                "date22", "date27", "date_1", "date_2", "date_3", "date45", "date_14", "date_26", "date2", "formula1"
            ]
            json_data = []
            df_produccion = fetch_full_data()


            board_id = 2354185091
            from_date = (datetime.strptime(df_produccion['Fecha Inicio ODT'].min(), '%Y-%m-%d') - timedelta(days=2)).strftime('%Y-%m-%d')
            to_date = datetime.now(timezone.utc)  # Use the current date as the end date, ensuring it's timezone-aware
            df_activity_logs = fetch_activity_logs(api_key, board_id, from_date, to_date)
            df_activity_logs.rename(columns={'pulse_id': 'Item ID'}, inplace=True)
            st.text("---------- Dataframe de JSON Queries creado ----------")

            df_final = dataframes_cross_active(df_produccion, df_activity_logs)
            st.text("---------- Cruce de dataframes realizado con éxito ----------")

            df_cerrados = task_time(df_final)
            st.text("---------- Cálculo de duración de actividades completado ----------")

            st.dataframe(df_cerrados)

            if st.button("Cargar", key="carga_cer"):
                worksheet = sh.worksheet("Proyectos CERRADOS")
                worksheet.update('A1', "PRUEBA")
                # set_with_dataframe(worksheet, df_cerrados)
        


# Tab para Proyectos Cerrados
with tab_cerrado:
    if st.button("DF", key="Cerrados"):
            board_ids = ["2354185091"]  # Ejemplo de lista de IDs de tableros
            group_ids = ["new_group11120"]  # Ejemplo de lista de IDs de grupos: "new_group11120"
            api_key = 'eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjQwNTEwNjk2OCwiYWFpIjoxMSwidWlkIjo2Mzg5NDk0MCwiaWFkIjoiMjAyNC0wOS0wMlQxODoyNjo0OS4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MTk3MDA2NSwicmduIjoidXNlMSJ9.XSia7vseMdnGXBQ2PiCjYNUtch-bOxeXQZeXv_8q1iI'  # Reemplaza esto con tu token de API real
            column_ids = [
                "text8", "date5", "date20", "fecha", "dropdown6", "label", "dup__of_status_17", "status_1",
                "dup__of_status_10", "dup__of_status_11", "dup__of_status_19", "dup__of_empaque", "status_14",
                "date22", "date27", "date_1", "date_2", "date_3", "date45", "date_14", "date_26", "date2", "formula1"
            ]
            json_data = []
            df_produccion = fetch_full_data()


            board_id = 2354185091
            from_date = (datetime.strptime(df_produccion['Fecha Inicio ODT'].min(), '%Y-%m-%d') - timedelta(days=2)).strftime('%Y-%m-%d')
            to_date = datetime.now(timezone.utc)  # Use the current date as the end date, ensuring it's timezone-aware
            df_activity_logs = fetch_activity_logs(api_key, board_id, from_date, to_date)
            df_activity_logs.rename(columns={'pulse_id': 'Item ID'}, inplace=True)
            st.text("---------- Dataframe de JSON Queries creado ----------")

            df_final = dataframes_cross_full(df_produccion, df_activity_logs)
            st.text("---------- Cruce de dataframes realizado con éxito ----------")

            df_activos = task_time(df_final)
            st.text("---------- Cálculo de duración de actividades completado ----------")

            result = clean_data(df_activos)
            st.dataframe(result)

