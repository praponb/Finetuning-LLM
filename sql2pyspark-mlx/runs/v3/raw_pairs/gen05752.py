from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type", "page")
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.sum(F.when(F.col("event_type") == "search", F.col("duration_sec")).otherwise(0)).alias("search_duration_sec"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
    )
)
