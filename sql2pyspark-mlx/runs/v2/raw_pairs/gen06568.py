from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.monthly_fee"),
        F.col("s.start_date"),
        F.col("s.status"),
        F.col("c.city"),
    )
)
