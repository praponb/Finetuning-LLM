from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") < "2025-07-01") & (F.col("event_type") == "page_view") & F.col("event_type").isin(["purchase", "click"]))
    .groupBy("session_id")
    .agg(
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.max("duration_sec").alias("max_duration_sec"),
        F.min("event_time").alias("first_event_time"),
    )
)
