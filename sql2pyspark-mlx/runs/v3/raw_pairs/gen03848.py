from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "salary",
        "is_remote",
        "manager_id",
        F.last_day("hire_date").alias("hire_date_month_end"),
        F.col("is_remote").cast("int").alias("is_remote_flag"),
    )
)
