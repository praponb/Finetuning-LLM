from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("duration_sec").between(171, 430))
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("page") == "/search", 1).otherwise(0)).alias("search_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("s.browser"),
        F.col("t.search_count"),
    )
)
