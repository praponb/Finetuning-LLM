from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.hour(F.col("we.event_time")).between(10, 18))
    .groupBy(F.year(F.col("s.started_at")).alias("started_year"))
    .agg(
        F.max(F.col("we.duration_sec")).alias("max_duration_sec"),
        F.round(F.stddev(F.col("we.duration_sec")), 2).alias("stddev_duration_sec"),
    )
    .orderBy("stddev_duration_sec")
)
