from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("page_view_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .filter(F.col("t.page_view_count") > 28)
    .select(F.col("s.device"), F.col("t.page_view_count"))
    .orderBy(F.col("device").desc(), F.col("page_view_count").desc())
)
