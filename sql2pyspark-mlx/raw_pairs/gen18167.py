from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").isNull() & ~F.col("event_type").isin(["purchase", "click"]))
    .groupBy("event_type", "page")
    .agg(
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy("page", "event_type")
    .limit(3)
)
