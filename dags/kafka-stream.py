from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Khokiat',
    'start_date': days_ago(1),  # You might want to adjust the start_date based on your requirements
}
def get_data ():
    import requests

    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0] ###dict object
    return res

def format_data(res):
    data ={}
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['name']
    data['email'] = res['email']

    return data

def stream_data():
    import json
    res = get_data()
    res = format_data(res)
    print(json.dumps(res, indent=3)) ##Use json dump to change from dict to json

with DAG('User_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    stream_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )

stream_data();
