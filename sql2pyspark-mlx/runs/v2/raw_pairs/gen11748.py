from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("session_id").isin([14, 38, 40]))
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("page") == "/search", 1).otherwise(0)).alias("search_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .filter(F.col("ss.search_count") > 57)
    .select(F.col("s.browser"), F.col("s.started_at"), F.col("ss.search_count"))
    .orderBy("browser")
)
