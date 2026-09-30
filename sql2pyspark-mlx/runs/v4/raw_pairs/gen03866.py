from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.col("duration_sec").between(168, 337))
    .groupBy("session_id")
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.min("event_time").alias("first_event_time"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .filter(F.col("ss.avg_duration_sec") >= 420)
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("s.started_at"),
        F.col("ss.avg_duration_sec"),
        F.col("ss.first_event_time"),
    )
    .orderBy(F.col("browser").desc())
)
