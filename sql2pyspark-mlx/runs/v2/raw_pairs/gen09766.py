from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.alias("web_events"), F.col("web_events.session_id") == F.col("sessions.id"), "left")
    .filter(F.col("sessions.country").isin(["US", "DE", "TH"]))
    .groupBy(F.col("sessions.device"))
    .agg(
        F.count(F.col("web_events.id")).alias("web_event_count"),
        F.round(F.stddev(F.col("web_events.duration_sec")), 2).alias("stddev_duration_sec"),
        F.count(F.when(F.col("web_events.event_type") == "click", 1)).alias("click_count"),
    )
    .orderBy(F.col("click_count").desc())
)
