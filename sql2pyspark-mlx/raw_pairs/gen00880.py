from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("page").isin(["/cart", "/product", "/home"]))
    .groupBy("session_id")
    .agg(
        F.min("event_time").alias("min_event_time"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .filter(F.col("t.avg_duration_sec") >= 38)
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("t.min_event_time"),
        F.col("t.avg_duration_sec"),
    )
    .orderBy(F.col("id").desc())
    .limit(3)
)
