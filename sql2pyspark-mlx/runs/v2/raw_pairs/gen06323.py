from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.filter(F.col("event_time") < "2024-11-21").alias("web_events"), F.col("sessions.id") == F.col("web_events.session_id"), "left_anti")
    .filter(F.col("sessions.started_at") < "2023-03-04")
    .select(F.col("sessions.id"), F.col("sessions.device"))
)
