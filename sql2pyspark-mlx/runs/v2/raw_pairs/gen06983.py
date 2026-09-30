from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("event_type").isin(["page_view", "add_to_cart"]))
    .groupBy("session_id")
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .select(
        F.col("s.country"),
        F.col("t.stddev_duration_sec"),
        F.col("t.checkout_count"),
    )
    .orderBy(F.col("checkout_count").desc())
)
