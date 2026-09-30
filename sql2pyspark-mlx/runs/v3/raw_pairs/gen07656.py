from pyspark.sql import functions as F

filtered_web_events = (
    df_web_events
    .filter((F.col("event_type").isin(["purchase", "add_to_cart", "search"]) & F.hour("event_time").between(7, 13)) | (F.col("event_type") == "add_to_cart"))
)

summary = (
    filtered_web_events
    .groupBy("event_type")
    .agg(
        F.min("event_time").alias("min_event_time"),
        F.countDistinct("duration_sec").alias("unique_sessions"),
    )
)

result = (
    summary
    .select("event_type", "unique_sessions")
    .orderBy("event_type")
    .limit(1)
)
