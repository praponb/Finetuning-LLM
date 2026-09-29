from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.length(F.col("c.name")) < 10)
    .select(
        F.col("c.age"),
        F.col("c.segment"),
        F.col("s.customer_id"),
        F.col("s.status"),
    )
    .orderBy("age")
)
