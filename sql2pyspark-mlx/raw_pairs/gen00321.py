from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("duration_sec").isNull())
    .groupBy("session_id")
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.sum(F.when(F.col("page") == "/home", 1).otherwise(0)).alias("home_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .select(
        F.col("s.started_at"),
        F.col("t.stddev_duration_sec"),
        F.col("t.home_count"),
    )
    .orderBy("started_at")
)
