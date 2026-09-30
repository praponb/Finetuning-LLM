from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") > 63)
    .select(
        "id",
        "credit_limit",
        F.last_day("signup_date").alias("signup_date_month_end"),
        F.length("city").alias("city_length"),
    )
)
