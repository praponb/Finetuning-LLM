from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "right")
    .select(
        F.col("s.id"),
        F.col("s.status"),
        F.col("s.monthly_fee"),
        F.col("s.end_date"),
        F.col("c.email"),
    )
)
