from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("event_type") == "add_to_cart")
    .groupBy("session_id")
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.sum(F.when(F.col("page") == "/cart", F.col("duration_sec")).otherwise(0)).alias("cart_duration_sec"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_stats.alias("session_stats"), F.col("sessions.id") == F.col("session_stats.session_id"), "inner")
    .select(
        F.col("sessions.id"),
        F.col("sessions.device"),
        F.col("session_stats.avg_duration_sec"),
        F.col("session_stats.cart_duration_sec"),
    )
)
