from pyspark.sql import functions as F

recent_web_events = (
    df_web_events
    .filter((F.col("event_type") == "add_to_cart") & F.col("event_type").isin(["search", "add_to_cart", "purchase"]))
    .withColumn("event_time_label", F.date_format("event_time", "MMM yyyy"))
)

web_events_stats = (
    recent_web_events
    .groupBy("event_type", "session_id")
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
)

result = (
    web_events_stats
    .filter(F.col("total_duration_sec") >= 1800)
    .select("event_type", "session_id")
    .orderBy(F.col("event_type").desc())
)
