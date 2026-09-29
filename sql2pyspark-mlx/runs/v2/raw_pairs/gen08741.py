from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "outer")
    .groupBy(F.col("c.country"))
    .agg(
        F.count("*").alias("total_count"),
        F.max(F.col("s.started_at")).alias("last_started_at"),
    )
    .orderBy("last_started_at")
)
