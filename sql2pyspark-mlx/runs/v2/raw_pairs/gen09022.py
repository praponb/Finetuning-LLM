from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left")
    .select(
        F.col("c.age"),
        F.col("c.city"),
        F.col("s.customer_id"),
        F.col("s.plan"),
        F.col("s.status"),
    )
)
