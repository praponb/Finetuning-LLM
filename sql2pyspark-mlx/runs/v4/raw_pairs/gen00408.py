from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("event_type") == "purchase")
    .groupBy("session_id")
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.countDistinct("duration_sec").alias("unique_sessions"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("t"), F.col("s.id") == F.col("t.session_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("t.avg_duration_sec"),
        F.col("t.unique_sessions"),
    )
    .orderBy(F.col("id").desc())
)
