from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter((F.col("c.signup_date") >= "2025-09-01") & (F.col("s.start_date") >= "2024-01-01"))
    .select(
        F.col("s.id"),
        F.col("s.end_date"),
        F.col("s.monthly_fee"),
        F.col("s.customer_id"),
        F.col("c.name"),
        F.col("c.age"),
        F.floor(F.col("c.credit_limit")).alias("credit_limit_whole"),
    )
)
