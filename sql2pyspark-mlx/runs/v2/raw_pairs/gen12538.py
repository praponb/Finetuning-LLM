from pyspark.sql import functions as F

recent_web_events = (
    df_web_events
    .filter((F.col("event_type") == "add_to_cart") & (F.col("duration_sec") < 292) & (F.col("session_id") == 10))
)

web_events_stats = (
    recent_web_events
    .groupBy("page", F.month("event_time").alias("event_month"))
    .agg(
        F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
    )
)

result = (
    web_events_stats
    .select("page", "event_month", "purchase_count")
    .orderBy("page")
)
