from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("s.device") == "desktop")
    .groupBy(F.col("c.city"))
    .agg(
        F.avg(F.col("we.duration_sec")).alias("avg_duration_sec"),
        F.sum(F.col("we.duration_sec")).alias("total_duration_sec"),
    )
)
