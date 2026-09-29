from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_totals.alias("session_totals"), F.col("sessions.id") == F.col("session_totals.session_id"), "inner")
    .filter(F.col("session_totals.purchase_count") >= 80)
    .select(
        F.col("sessions.id"),
        F.col("sessions.customer_id"),
        F.col("session_totals.purchase_count"),
    )
)
