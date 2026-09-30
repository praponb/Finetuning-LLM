from pyspark.sql import functions as F

filtered_web_events = (
    df_web_events
    .filter((F.col("duration_sec") == 311) & (F.col("page") == "/checkout"))
)

web_events_stats = (
    filtered_web_events
    .groupBy("session_id", "page")
    .agg(
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.countDistinct("duration_sec").alias("unique_event_types"),
    )
)

result = (
    web_events_stats
    .filter(F.col("unique_event_types") >= 9)
    .select("session_id", "page", "page_list")
)
