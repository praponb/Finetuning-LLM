from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(
        F.count("*").alias("row_count"),
        F.max("signup_date").alias("latest_signup_date"),
    )
)
