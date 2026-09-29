from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.countDistinct("duration_sec").alias("unique_sessions"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("s.started_at"),
        F.col("t.max_duration_sec"),
        F.col("t.unique_sessions"),
    )
    .orderBy(F.col("browser").desc(), F.col("unique_sessions").desc(), "id")
    .limit(1)
)
