from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .groupBy(F.col("s.customer_id"))
    .agg(
        F.min(F.col("we.event_time")).alias("first_event_time"),
        F.round(F.stddev(F.col("we.duration_sec")), 2).alias("stddev_duration_sec"),
    )
    .orderBy(F.col("stddev_duration_sec").desc(), "first_event_time")
)
