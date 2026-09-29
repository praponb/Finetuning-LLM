from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("page") == "/product", 1).otherwise(0))) / F.count("*")), 1).alias("pct_product"),
        F.sum(F.when(F.col("event_type") == "add_to_cart", F.col("duration_sec")).otherwise(0)).alias("add_to_cart_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .filter(F.col("t.pct_product") >= 46)
    .select(
        F.col("s.customer_id"),
        F.col("t.pct_product"),
        F.col("t.add_to_cart_duration_sec"),
    )
    .orderBy("pct_product", F.col("customer_id").desc())
)
