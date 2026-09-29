from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "left")
    .groupBy(F.col("sessions.browser"))
    .agg(
        F.countDistinct(F.col("web_events.duration_sec")).alias("unique_event_types"),
        F.min(F.col("web_events.event_time")).alias("min_event_time"),
    )
)
