from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "outer")
    .select(
        F.col("s.start_date"),
        F.col("s.end_date"),
        F.col("s.monthly_fee"),
        F.col("c.country"),
        F.col("c.city"),
    )
)
