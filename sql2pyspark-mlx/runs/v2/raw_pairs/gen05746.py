from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.to_date("event_time") >= "2024-08-01")
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("page") == "/cart", 1).otherwise(0)).alias("cart_count"),
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_stats.alias("t"), F.col("sessions.id") == F.col("t.session_id"), "inner")
    .select(
        F.col("sessions.id"),
        F.col("sessions.country"),
        F.col("t.cart_count"),
        F.col("t.checkout_count"),
    )
    .orderBy(F.col("id").desc(), "country")
)
