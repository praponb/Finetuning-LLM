from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.col("session_id").isin([3, 22, 38]))
    .groupBy("session_id")
    .agg(
        F.min("duration_sec").alias("min_duration_sec"),
        F.round(((100 * F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0))) / F.count("*")), 1).alias("pct_add_to_cart"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("t"), F.col("s.id") == F.col("t.session_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("s.browser"),
        F.col("t.min_duration_sec"),
        F.col("t.pct_add_to_cart"),
    )
)
