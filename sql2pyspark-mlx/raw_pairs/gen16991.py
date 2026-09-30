from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.filter(F.col("signup_date") >= "2025-01-27").alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("s.customer_id").isin([24, 35, 37, 38]))
    .select(
        F.col("s.id"),
        F.col("s.start_date"),
        F.col("s.customer_id"),
        F.col("s.plan"),
    )
)
