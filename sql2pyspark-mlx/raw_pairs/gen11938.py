from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "outer")
    .groupBy(F.col("c.segment"))
    .agg(
        F.max(F.col("s.started_at")).alias("latest_started_at"),
        F.countDistinct(F.col("s.device")).alias("unique_devices"),
    )
    .orderBy("latest_started_at")
)
