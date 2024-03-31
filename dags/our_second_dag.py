from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner':'Riya',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}
with DAG(
    dag_id='our_second_dag',
    default_args=default_args,
    description='This is our second dag that we write',
    start_date=datetime(2021, 7, 29, 2),
    schedule_interval='@daily'


) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo hello world, this is the first task of second DAG!"
    )
    task1