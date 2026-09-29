from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.col("page") == "/product")
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("page") == "/search", 1).otherwise(0)).alias("search_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .filter(F.col("t.search_count") >= 35)
    .select(F.col("s.id"), F.col("s.started_at"), F.col("t.search_count"))
)
