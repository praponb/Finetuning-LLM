from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "left")
    .groupBy(F.col("sessions.browser"), F.col("sessions.device"))
    .agg(
        F.max(F.col("web_events.duration_sec")).alias("max_duration_sec"),
        F.count(F.when(F.col("web_events.event_type") == "click", 1)).alias("click_count"),
    )
    .filter(F.col("max_duration_sec") > 330)
)
