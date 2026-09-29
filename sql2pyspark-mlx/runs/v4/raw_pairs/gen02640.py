from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.alias("web_events"), F.col("sessions.id") == F.col("web_events.session_id"), "left_semi")
    .filter(F.col("sessions.browser").isNotNull())
    .select(F.col("sessions.id"), F.col("sessions.device"))
)
