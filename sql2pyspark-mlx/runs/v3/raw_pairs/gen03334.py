from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .groupBy(
        F.col("s.device"),
        F.date_trunc("MONTH", F.col("s.started_at")).alias("started_month"),
    )
    .agg(
        F.round(F.stddev(F.col("we.duration_sec")), 2).alias("stddev_duration_sec"),
        F.min(F.col("we.duration_sec")).alias("min_duration_sec"),
    )
    .orderBy("stddev_duration_sec")
)
