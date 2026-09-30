from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.filter(F.col("customer_id") == 8).alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "left_semi")
    .select(
        F.col("web_events.id"),
        F.col("web_events.duration_sec"),
        F.col("web_events.event_time"),
        F.col("web_events.page"),
    )
)
