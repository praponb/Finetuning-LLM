from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("event_type").isin(["click", "add_to_cart"]))
    .groupBy("session_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("page") == "/checkout", 1).otherwise(0))) / F.count("*")), 1).alias("pct_checkout"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("S"), F.col("s.id") == F.col("S.session_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("s.customer_id"),
        F.col("S.pct_checkout"),
    )
    .orderBy(F.col("customer_id").desc(), "pct_checkout")
)
