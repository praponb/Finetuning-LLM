from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "right")
    .filter(F.col("web_events.page") == "/product")
    .select(
        F.col("web_events.duration_sec"),
        F.col("web_events.session_id"),
        F.col("sessions.device"),
        F.col("sessions.started_at"),
    )
    .orderBy(F.col("session_id").desc())
)
