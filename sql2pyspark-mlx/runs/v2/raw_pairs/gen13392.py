from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
    )
    .filter(F.col("stddev_duration_sec") >= 60)
    .select("page", "stddev_duration_sec", "event_type_values")
    .orderBy("stddev_duration_sec")
)
