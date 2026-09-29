from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("event_type").isin(["click", "search", "purchase"]))
    .groupBy("session_id")
    .agg(
        F.max("event_time").alias("last_event_time"),
        F.round(((100 * F.sum(F.when(F.col("page") == "/product", 1).otherwise(0))) / F.count("*")), 1).alias("pct_product"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .filter(F.col("ss.pct_product") >= 75)
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.customer_id"),
        F.col("ss.last_event_time"),
        F.col("ss.pct_product"),
    )
)
