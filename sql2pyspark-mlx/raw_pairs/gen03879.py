from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("event_type") == "add_to_cart", F.col("duration_sec")).otherwise(0)).alias("add_to_cart_duration_sec"),
        F.round(((100 * F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0))) / F.count("*")), 1).alias("pct_click"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("S"), F.col("s.id") == F.col("S.session_id"), "left")
    .filter(F.col("S.pct_click") >= 49)
    .select(
        F.col("s.customer_id"),
        F.col("S.add_to_cart_duration_sec"),
        F.col("S.pct_click"),
    )
    .orderBy("add_to_cart_duration_sec")
)
