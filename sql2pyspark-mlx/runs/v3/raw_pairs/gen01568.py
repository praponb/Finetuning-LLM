from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
        F.max("event_time").alias("latest_event_time"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("S"), F.col("s.id") == F.col("S.session_id"), "left")
    .filter(F.col("S.purchase_count") > 36)
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("s.started_at"),
        F.col("S.purchase_count"),
        F.col("S.latest_event_time"),
    )
)
