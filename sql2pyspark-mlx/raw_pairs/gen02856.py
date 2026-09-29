from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.device"), F.month(F.col("s.started_at")).alias("started_month"))
    .agg(
        F.min(F.col("we.event_time")).alias("first_event_time"),
        F.avg(F.col("we.duration_sec")).alias("avg_duration_sec"),
    )
    .orderBy("avg_duration_sec")
)
