from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("t"), F.col("s.id") == F.col("t.session_id"), "left")
    .filter(F.col("t.add_to_cart_count") >= 54)
    .select(
        F.col("s.device"),
        F.col("t.avg_duration_sec"),
        F.col("t.add_to_cart_count"),
    )
    .orderBy(F.col("add_to_cart_count").desc())
)
