from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(F.avg("duration_sec").alias("avg_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("W"), F.col("s.id") == F.col("W.session_id"), "inner")
    .filter(F.col("W.avg_duration_sec") >= 150)
    .select(F.col("s.country"), F.col("s.customer_id"), F.col("W.avg_duration_sec"))
)
