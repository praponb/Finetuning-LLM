from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "inner")
    .filter(F.col("sessions.started_at") >= "2023-04-12")
    .groupBy(F.col("sessions.browser"), F.col("sessions.country"))
    .agg(
        F.sum(F.col("web_events.duration_sec")).alias("total_duration_sec"),
        F.sum(F.when(F.col("web_events.event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
    )
    .filter(F.col("total_duration_sec") > 2600)
)
