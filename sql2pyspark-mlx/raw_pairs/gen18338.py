from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .filter(F.hour(F.col("s.started_at")).between(7, 14))
    .groupBy(
        F.col("s.customer_id"),
        F.to_date(F.col("s.started_at")).alias("started_day"),
    )
    .agg(F.round(F.stddev(F.col("we.duration_sec")), 2).alias("stddev_duration_sec"))
    .filter(F.col("stddev_duration_sec") > 62)
)
