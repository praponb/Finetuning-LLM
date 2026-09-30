from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(
        F.count("*").alias("customer_count"),
        F.max("signup_date").alias("latest_signup_date"),
        (F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"),
    )
    .orderBy("credit_limit_range")
)
