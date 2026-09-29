from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.max("event_time").alias("latest_event_time"),
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
    )
    .filter(F.col("total_duration_sec") < 2000)
)
