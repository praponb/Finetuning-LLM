from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.filter(F.col("device") == "desktop").alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "left_anti")
    .filter(F.col("web_events.duration_sec") >= 452)
    .select(
        F.col("web_events.id"),
        F.col("web_events.page"),
        F.col("web_events.session_id"),
        F.col("web_events.event_time"),
    )
)
