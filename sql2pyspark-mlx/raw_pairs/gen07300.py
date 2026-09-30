from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.alias("web_events"), F.col("web_events.session_id") == F.col("sessions.id"), "left")
    .filter(F.to_date(F.col("web_events.event_time")) >= "2024-11-27")
    .groupBy(F.col("sessions.country"))
    .agg(
        F.count(F.col("web_events.id")).alias("web_event_count"),
        F.sum(F.col("web_events.duration_sec")).alias("total_duration_sec"),
    )
)
