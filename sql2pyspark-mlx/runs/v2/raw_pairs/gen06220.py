from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("session_id").isin([17, 30]))
    .groupBy("session_id")
    .agg(
        F.count(F.when(F.col("event_type") == "page_view", 1)).alias("page_view_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "left")
    .select(F.col("s.country"), F.col("s.customer_id"), F.col("ss.page_view_count"))
)
