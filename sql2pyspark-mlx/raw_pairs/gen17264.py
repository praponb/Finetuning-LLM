from pyspark.sql import functions as F

recent_web_events = df_web_events.filter(F.to_date("event_time") < "2024-09-30")

web_events_stats = (
    recent_web_events
    .groupBy("page", F.hour("event_time").alias("event_hour"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
)

result = (
    web_events_stats
    .filter(F.col("min_duration_sec") < 180)
    .orderBy("page_values")
)
