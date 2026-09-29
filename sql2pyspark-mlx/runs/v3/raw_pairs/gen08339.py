from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("c.age"),
        F.col("c.city"),
        F.col("c.email"),
        F.col("s.monthly_fee"),
        F.col("s.status"),
    )
)
