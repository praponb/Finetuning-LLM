from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.alias("web_events"), F.col("sessions.id") == F.col("web_events.session_id"), "left")
    .groupBy(F.col("sessions.id"), F.col("sessions.device"))
    .agg(F.count(F.col("web_events.id")).alias("web_event_count"))
    .filter(F.col("web_event_count") > 2)
)
