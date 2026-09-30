from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "inner")
    .filter(F.col("sessions.country") == "TH")
    .groupBy(
        F.to_date(F.col("sessions.started_at")).alias("started_day"),
        F.date_trunc("MONTH", F.col("sessions.started_at")).alias("started_month"),
    )
    .agg(
        F.max(F.col("web_events.event_time")).alias("last_event_time"),
        F.avg(F.col("web_events.duration_sec")).alias("avg_duration_sec"),
    )
    .orderBy("started_month", "started_day")
)
