from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_count"),
        F.min("event_time").alias("min_event_time"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_summary.alias("session_summary"), F.col("sessions.id") == F.col("session_summary.session_id"), "inner")
    .filter(F.col("session_summary.add_to_cart_count") >= 39)
    .select(
        F.col("sessions.started_at"),
        F.col("sessions.device"),
        F.col("session_summary.add_to_cart_count"),
        F.col("session_summary.min_event_time"),
    )
    .orderBy("started_at", F.col("min_event_time").desc())
)
