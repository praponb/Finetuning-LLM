from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter(F.col("c.city") != "Tokyo")
    .groupBy(F.col("c.city"), F.col("c.is_active"))
    .agg(F.max(F.col("s.started_at")).alias("last_started_at"))
    .orderBy("city", "last_started_at")
)
