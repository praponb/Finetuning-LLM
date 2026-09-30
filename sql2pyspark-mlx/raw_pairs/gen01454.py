from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "salary",
        "full_name",
        "job_title",
        F.last_day("hire_date").alias("hire_date_month_end"),
    )
)
