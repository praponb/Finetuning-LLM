from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "outer")
    .select(
        F.col("web_events.event_time"),
        F.col("web_events.session_id"),
        F.col("web_events.duration_sec"),
        F.col("sessions.started_at"),
        F.col("sessions.browser"),
    )
)
