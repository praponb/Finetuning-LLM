from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "manager_id",
        "hire_date",
        "full_name",
        F.last_day("hire_date").alias("hire_date_month_end"),
    )
)
