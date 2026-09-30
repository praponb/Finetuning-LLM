from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "outer")
    .filter(F.hour(F.col("we.event_time")).between(6, 11))
    .groupBy(F.year(F.col("s.started_at")).alias("started_year"))
    .agg(F.avg(F.col("we.duration_sec")).alias("avg_duration_sec"))
)
