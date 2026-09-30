from pyspark.sql import functions as F

result = (
    df_web_events
    .cube("event_type", "session_id")
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
    )
)
