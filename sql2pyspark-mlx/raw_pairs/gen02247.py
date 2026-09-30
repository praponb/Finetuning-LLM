from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "email",
        F.last_day("hire_date").alias("hire_date_month_end"),
        F.substring("email", 1, 3).alias("email_prefix"),
    )
)
