from pyspark.sql import functions as F

customer_stats = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.max("started_at").alias("last_started_at"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.is_active"),
        F.col("c.email"),
        F.col("t.last_started_at"),
    )
)
