from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("add_to_cart_count"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_summary.alias("session_summary"), F.col("sessions.id") == F.col("session_summary.session_id"), "inner")
    .filter(F.col("session_summary.add_to_cart_count") >= 4)
    .select(
        F.col("sessions.id"),
        F.col("sessions.started_at"),
        F.col("session_summary.add_to_cart_count"),
    )
    .orderBy(F.col("add_to_cart_count").desc())
)
