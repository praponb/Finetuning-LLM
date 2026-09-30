from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.alias("web_events"), F.col("web_events.session_id") == F.col("sessions.id"), "left")
    .filter(F.col("web_events.session_id") == 10)
    .groupBy(F.col("sessions.browser"))
    .agg(F.count(F.col("web_events.id")).alias("web_event_count"))
)
