from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.filter(F.to_date("event_time") < "2025-09-06").alias("web_events"), F.col("sessions.id") == F.col("web_events.session_id"), "left_semi")
    .filter(F.hour(F.col("sessions.started_at")).between(11, 17))
    .select(
        F.col("sessions.id"),
        F.col("sessions.browser"),
        F.col("sessions.device"),
        F.col("sessions.country"),
    )
)
