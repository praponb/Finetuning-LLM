from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("event_type").isin(["search", "add_to_cart", "page_view"]))
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("page_view_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("s.device"),
        F.col("t.page_view_count"),
    )
    .orderBy(F.col("id").desc(), F.col("device").desc())
)
