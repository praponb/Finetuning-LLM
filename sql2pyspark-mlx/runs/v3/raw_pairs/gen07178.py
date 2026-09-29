from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .select(
        "job_title",
        "full_name",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.last_day("hire_date").alias("hire_date_month_end"),
    )
)
