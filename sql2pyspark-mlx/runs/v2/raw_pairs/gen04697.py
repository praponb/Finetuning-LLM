from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("event_type") == "add_to_cart", F.col("duration_sec")).otherwise(0)).alias("add_to_cart_duration_sec"),
        F.round(((100 * F.sum(F.when(F.col("page") == "/cart", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cart"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("webs.add_to_cart_duration_sec"),
        F.col("webs.pct_cart"),
    )
)
