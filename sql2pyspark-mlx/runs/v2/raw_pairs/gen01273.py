from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([2, 17]) & (F.col("event_time") < "2023-10-16") & F.col("session_id").isin([7, 13, 19, 24]))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
)
