from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .groupBy(
        F.col("s.country"),
        F.date_trunc("MONTH", F.col("s.started_at")).alias("started_month"),
    )
    .agg(
        F.round(F.stddev(F.col("we.duration_sec")), 2).alias("stddev_duration_sec"),
        F.countDistinct(F.col("we.duration_sec")).alias("unique_pages"),
    )
    .orderBy(F.col("country").desc(), "started_month")
    .limit(1)
)
