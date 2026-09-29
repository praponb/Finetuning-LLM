from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("event_type").isin(["click", "add_to_cart", "page_view"]))
    .groupBy("session_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0))) / F.count("*")), 1).alias("pct_add_to_cart"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "left")
    .filter(F.col("ss.pct_add_to_cart") > 55)
    .select(F.col("s.id"), F.col("s.customer_id"), F.col("ss.pct_add_to_cart"))
    .orderBy("pct_add_to_cart", F.col("customer_id").desc())
)
