from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("event_type") == "add_to_cart")
    .groupBy("session_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
        F.round(((100 * F.sum(F.when(F.col("page") == "/checkout", 1).otherwise(0))) / F.count("*")), 1).alias("pct_checkout"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .filter(F.col("t.pct_checkout") >= 26)
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("s.customer_id"),
        F.col("t.page_values"),
        F.col("t.pct_checkout"),
    )
    .orderBy(F.col("customer_id").desc())
)
