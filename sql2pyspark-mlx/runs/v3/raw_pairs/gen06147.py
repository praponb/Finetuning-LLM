from pyspark.sql import functions as F

customer_stats = (
    df_sessions
    .filter(F.col("device").isin(["desktop", "tablet", "mobile"]))
    .groupBy("customer_id")
    .agg(F.min("started_at").alias("min_started_at"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.segment"),
        F.col("c.city"),
        F.col("t.min_started_at"),
    )
)
