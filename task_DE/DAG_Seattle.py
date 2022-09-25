from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'retry': 5,
    'retry_delay': timedelta(minutes=5)
}


def cleaning_data():
    import codecs
    f = codecs.open("task_DE.ipynb", 'r')
    print("Data cleaning")


def spark_processing():
    import codecs
    c = codecs.open("Spark_preprocessing.ipynb", 'r')
    print("Data processing in spark")


def visualization():
    import codecs
    v = codecs.open("Vizualization_spark_results.ipynb", 'r')
    print("Visualization results of Seattle public")


with DAG(
        default_args=default_args,
        dag_id="Seattle_public",
        start_date=datetime(2022, 9, 24),
        schedule_interval='@daily'
) as dag:
    data_cleaned = PythonOperator(
        task_id='cleaning_data',
        python_callable=cleaning_data
    )

    spark_data_processed = PythonOperator(
        task_id='spark_processing',
        python_callable=spark_processing
    )

    visualization_result = PythonOperator(
        task_id='visualization',
        python_callable=visualization
    )

