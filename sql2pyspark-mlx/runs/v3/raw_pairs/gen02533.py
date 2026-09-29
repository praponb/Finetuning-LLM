from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.alias("web_events"), F.col("web_events.session_id") == F.col("sessions.id"), "inner")
    .filter(F.col("sessions.started_at") >= "2024-07-01")
    .groupBy(F.col("sessions.id"))
    .agg(
        F.count(F.col("web_events.id")).alias("web_event_count"),
        F.avg(F.col("web_events.duration_sec")).alias("avg_duration_sec"),
    )
    .filter(F.col("web_event_count") >= 5)
)
