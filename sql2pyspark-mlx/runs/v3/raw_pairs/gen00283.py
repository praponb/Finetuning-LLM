from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.min("duration_sec").alias("min_duration_sec"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("W"), F.col("s.id") == F.col("W.session_id"), "inner")
    .filter(F.col("W.min_duration_sec") >= 340)
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("W.min_duration_sec"),
        F.col("W.total_duration_sec"),
    )
    .orderBy("min_duration_sec", "id")
    .limit(20)
)
