from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id") == 32)
    .groupBy("page", "event_type", F.to_date("event_time").alias("event_day"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
    )
    .orderBy(F.col("event_day").desc(), "page", "event_type")
    .limit(10)
)
