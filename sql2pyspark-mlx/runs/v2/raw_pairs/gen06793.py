from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "left")
    .filter(~F.col("web_events.event_type").isin(["page_view", "click"]))
    .groupBy(F.to_date(F.col("sessions.started_at")).alias("started_day"))
    .agg(
        F.sum(F.when(F.col("web_events.event_type") == "click", 1).otherwise(0)).alias("click_count"),
        F.round(F.avg(F.col("web_events.duration_sec")), 2).alias("avg_duration_sec"),
    )
)
