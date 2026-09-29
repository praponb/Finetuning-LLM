from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.min("duration_sec").alias("min_duration_sec"),
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
    )
    .filter(F.col("min_duration_sec") < 340)
    .select("session_id", "min_duration_sec", "event_type_values")
    .orderBy(F.col("min_duration_sec").desc())
)
