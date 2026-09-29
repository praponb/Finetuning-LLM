from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .filter(F.col("t.stddev_duration_sec") > 21)
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("s.customer_id"),
        F.col("t.stddev_duration_sec"),
    )
)
