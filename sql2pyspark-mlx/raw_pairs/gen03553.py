from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.count("*").alias("total_count"),
        F.count(F.when(F.col("event_type") == "search", 1)).alias("search_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .filter(F.col("ss.search_count") > 3)
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("s.customer_id"),
        F.col("ss.total_count"),
        F.col("ss.search_count"),
    )
)
