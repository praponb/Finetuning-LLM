from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.alias("web_events"), F.col("web_events.session_id") == F.col("sessions.id"), "left")
    .filter((F.to_date(F.col("sessions.started_at")) >= "2024-05-01") & (F.to_date(F.col("web_events.event_time")) < "2025-09-01"))
    .select(
        F.col("sessions.device"),
        F.col("web_events.event_type"),
        F.col("web_events.event_time"),
    )
    .orderBy("device")
)
