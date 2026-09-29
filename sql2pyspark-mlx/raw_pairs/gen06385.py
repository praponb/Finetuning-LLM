from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.filter(F.col("country").isin(["TH", "UK", "DE"])).alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "left_anti")
    .select(
        F.col("web_events.id"),
        F.col("web_events.event_type"),
        F.col("web_events.duration_sec"),
        F.col("web_events.event_time"),
    )
    .orderBy(F.col("event_time").desc(), "id")
    .limit(50)
)
