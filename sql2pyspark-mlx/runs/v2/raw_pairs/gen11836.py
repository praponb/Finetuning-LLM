from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("page") == "/cart", 1).otherwise(0)).alias("cart_count"),
        F.sum(F.when(F.col("page") == "/search", F.col("duration_sec")).otherwise(0)).alias("search_duration_sec"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_stats.alias("SessionStats"), F.col("sessions.id") == F.col("SessionStats.session_id"), "inner")
    .filter(F.col("SessionStats.search_duration_sec") >= 69)
    .select(
        F.col("sessions.browser"),
        F.col("sessions.customer_id"),
        F.col("SessionStats.cart_count"),
        F.col("SessionStats.search_duration_sec"),
    )
)
