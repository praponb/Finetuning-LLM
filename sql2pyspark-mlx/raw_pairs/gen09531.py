from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("session_id").isin([23, 31, 35]))
    .groupBy("session_id")
    .agg(
        F.count("*").alias("total_count"),
        F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0)).alias("search_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("s.country"),
        F.col("ss.total_count"),
        F.col("ss.search_count"),
    )
    .orderBy(F.col("total_count").desc(), "country")
)
