from pyspark.sql import functions as F

filtered_web_events = (
    df_web_events
    .filter((F.col("duration_sec") <= 243) & (F.col("duration_sec").between(294, 447) | F.hour("event_time").between(11, 15)))
)

web_events_stats = (
    filtered_web_events
    .groupBy("event_type")
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.count(F.when(F.col("event_type") == "page_view", 1)).alias("page_view_count"),
    )
)

result = (
    web_events_stats
    .select("event_type", "page_view_count")
    .orderBy(F.col("page_view_count").desc())
)
