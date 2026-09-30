from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("session_id") == 15)
    .groupBy("session_id")
    .agg(
        F.count(F.when(F.col("page") == "/cart", 1)).alias("cart_count"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("s.customer_id"),
        F.col("ss.cart_count"),
        F.col("ss.avg_duration_sec"),
    )
    .orderBy("device", F.col("avg_duration_sec").desc())
)
