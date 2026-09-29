from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "outer")
    .filter(F.col("web_events.event_time") < "2023-04-19")
    .groupBy(F.col("sessions.browser"))
    .agg(
        F.round(F.avg(F.col("web_events.duration_sec")), 2).alias("avg_duration_sec"),
        F.max(F.col("web_events.event_time")).alias("latest_event_time"),
    )
    .filter(F.col("avg_duration_sec") >= 66)
    .orderBy(F.col("latest_event_time").desc(), "browser")
    .limit(50)
)
