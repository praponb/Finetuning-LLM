from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.col("event_time") < "2024-02-01")
    .groupBy("session_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("page") == "/cart", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cart"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("S"), F.col("s.id") == F.col("S.session_id"), "inner")
    .select(F.col("s.started_at"), F.col("s.customer_id"), F.col("S.pct_cart"))
    .orderBy(F.col("customer_id").desc(), F.col("pct_cart").desc())
)
