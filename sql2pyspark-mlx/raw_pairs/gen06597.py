from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("duration_sec") != 148)
    .groupBy("session_id")
    .agg(F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .filter(F.col("ss.stddev_duration_sec") >= 33)
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("s.browser"),
        F.col("ss.stddev_duration_sec"),
    )
    .orderBy(F.col("stddev_duration_sec").desc())
)
