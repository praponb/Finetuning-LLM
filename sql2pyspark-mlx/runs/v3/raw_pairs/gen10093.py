from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "right")
    .select(
        F.col("s.id"),
        F.col("s.monthly_fee"),
        F.col("c.city"),
        F.col("c.email"),
        F.col("c.segment"),
    )
)
