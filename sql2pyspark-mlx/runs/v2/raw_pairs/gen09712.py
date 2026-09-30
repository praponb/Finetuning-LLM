from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left")
    .groupBy(F.col("c.id"), F.col("c.city"))
    .agg(
        F.count(F.col("s.id")).alias("session_count"),
        F.max(F.col("s.started_at")).alias("latest_started_at"),
    )
    .orderBy("latest_started_at", "id", "city")
    .limit(50)
)
