from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.filter(F.col("duration_sec") <= 470).alias("web_events"), F.col("sessions.id") == F.col("web_events.session_id"), "left_anti")
    .filter(F.col("sessions.country") != "UK")
    .select(
        F.col("sessions.id"),
        F.col("sessions.device"),
        F.col("sessions.country"),
        F.col("sessions.started_at"),
    )
)
