from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.is_active"), F.col("c.city"))
    .agg(
        F.min(F.col("s.started_at")).alias("min_started_at"),
        F.max(F.col("s.started_at")).alias("latest_started_at"),
    )
    .orderBy("latest_started_at")
)
