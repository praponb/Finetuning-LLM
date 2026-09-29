from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
    )
)
