import datetime
import airflow
from airflow.operators import bash_operator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
default_args = {
    'owner': 'Composer Example',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': YESTERDAY,
}


ROWS_QUERY = (
f"""
  merge into `gcds-oht33435u3-2023`.`Ideathon_gcp`.`GOLD_PRODUCTS_DATA` as DBT_INTERNAL_DEST
    using( WITH SOURCE AS (
    SELECT
        CAST(unit_price AS INT64) as unit_price
        , color
        , CAST(weight AS FLOAT64) as weight
        , weight_unit
        , CAST(order_quantity AS INT64) as order_quantity
        , product_id
        , CAST(unit_price AS INT64) * CAST(order_quantity AS INT64) as sales
        , CURRENT_DATETIME() as LOAD_DATETIME
    FROM `gcds-oht33435u3-2023`.`Ideathon_gcp`.`RAW_PRODUCTS_DATA_EXT`
)

, CLEANUP AS (
    SELECT
        *
        , (
            CASE
            WHEN sales>10000 and EXTRACT(MONTH FROM LOAD_DATETIME)=12 and EXTRACT(DAY FROM LOAD_DATETIME)=25 and lower(color)='red'
                THEN sales*0.8
            WHEN sales>10000
                THEN sales*0.9
            ELSE sales
            END
        ) AS total_sales
        , 
    FROM SOURCE
)

SELECT distinct *
FROM CLEANUP
)  as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.product_id = DBT_INTERNAL_DEST.product_id
            )

        when matched then update set
        `unit_price` = DBT_INTERNAL_SOURCE.`unit_price`,`color` = DBT_INTERNAL_SOURCE.`color`,`weight` = DBT_INTERNAL_SOURCE.`weight`,`weight_unit` = DBT_INTERNAL_SOURCE.`weight_unit`,`order_quantity` = DBT_INTERNAL_SOURCE.`order_quantity`,`product_id` = DBT_INTERNAL_SOURCE.`product_id`,`sales` = DBT_INTERNAL_SOURCE.`sales`,`LOAD_DATETIME` = DBT_INTERNAL_SOURCE.`LOAD_DATETIME`,`total_sales` = DBT_INTERNAL_SOURCE.`total_sales`

    

    when not matched then insert
    (`unit_price`,`color`,`weight`,`weight_unit`,`order_quantity`,`product_id`,`sales`,`LOAD_DATETIME`,`total_sales`)
    values
    (`unit_price`,`color`,`weight`,`weight_unit`,`order_quantity`,`product_id`,`sales`,`LOAD_DATETIME`,`total_sales`)
"""
)

ORDERS_QUERY = (
f'''
  create or replace table gcds-oht33435u3-2023.Ideathon_gcp.PRODUCTS_DATA
  OPTIONS(
      description="""fetching data from GOLD_PRODUCTS_DATA table """
    )
  as WITH SOURCE AS (
    select * from gcds-oht33435u3-2023.Ideathon_gcp.GOLD_PRODUCTS_DATA
  )
  
, DEDUP AS (
    SELECT
        *
        , ROW_NUMBER() OVER (
            PARTITION BY
                product_id
            ORDER BY LOAD_DATETIME DESC
        ) AS RANK
    FROM SOURCE
)
  select * except(RANK) from DEDUP where RANK = 1
'''
)

with airflow.DAG(
        'af_orders',
        'catchup=False',
        default_args=default_args,
        schedule_interval=datetime.timedelta(days=1)) as dag:

    create_external_table = BigQueryCreateExternalTableOperator(
            task_id="create_external_table",
            destination_project_dataset_table="Ideathon_gcp.RAW_PRODUCTS_DATA_EXT",
            bucket="source_product_data",
            schema_fields=[
                {"name": "unit_price", "type": "STRING", "mode": "NULLABLE"},
                {"name": "color", "type": "STRING", "mode": "NULLABLE"},
                {"name": "weight", "type": "STRING", "mode": "NULLABLE"},
                {"name": "weight_unit", "type": "STRING", "mode": "NULLABLE"},
                {"name": "order_quantity", "type": "STRING", "mode": "NULLABLE"},
                {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
            ],
            source_objects=["MOCK_DATA.csv"],
        )
        
    insert_query_job = BigQueryInsertJobOperator(
        task_id="insert_query_job",
        configuration={
            "query": {
                "query": ROWS_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        location="us-central1",
    )
    
    orders_query_job = BigQueryInsertJobOperator(
        task_id="orders_query_job",
        configuration={
            "query": {
                "query": ORDERS_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        location="us-central1",
    )
    
    print_dag_run_conf = bash_operator.BashOperator(
        task_id='print_dag_run_conf', bash_command='echo {{ dag_run.id }}')

    create_external_table >> insert_query_job
    insert_query_job >> orders_query_job
    orders_query_job >> print_dag_run_conf
