from email.mime import base
from airflow import DAG
from datetime import date, datetime 
from airflow.operators.bash import BashOperator 
from airflow.operators.subdag import SubDagOperator
#from subdags.subdag_rarallel_dag import subdag_parallel_dag

default_args = {
            'start_date':datetime(2022, 4,28)
}

with DAG('parallel_dag', schedule_interval = '@daily', default_args = default_args, catchup= True) as dag:
    task1 = BashOperator(
        task_id = "task_1",
        bash_command = 'sleep 4'
    )
    task2 = BashOperator(
        task_id = 'task_2', 
        bash_command = 'sleep 4'
    )
    task3 = BashOperator(
        task_id = "task_3", 
        bash_command = 'sleep 5'
    )
    task_4 = BashOperator(
        task_id = 'task_4',
        bash_command = 'sleep 4'
    )
    # the local executor allow you to run parallel task 
    task1>>[task2,task3]>>task_4