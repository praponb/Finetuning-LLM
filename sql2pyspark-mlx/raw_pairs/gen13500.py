from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("s.status"),
        F.col("s.end_date"),
        F.col("c.city"),
        F.col("c.is_active"),
        F.col("c.age"),
    )
    .orderBy("city")
)
