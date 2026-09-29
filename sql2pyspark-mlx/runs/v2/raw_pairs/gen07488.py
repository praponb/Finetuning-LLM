from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page", F.hour("event_time").alias("event_hour"))
    .agg(
        F.max("event_time").alias("last_event_time"),
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
        F.count(F.when(F.col("event_type") == "click", 1)).alias("click_count"),
    )
)
