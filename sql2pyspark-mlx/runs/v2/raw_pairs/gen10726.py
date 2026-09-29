from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_time") < "2023-10-01")
    .groupBy("event_type", "session_id")
    .agg(
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
    )
)
