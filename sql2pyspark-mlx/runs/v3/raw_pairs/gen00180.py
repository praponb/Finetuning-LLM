from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type", "page")
    .agg(
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
    .orderBy("page_values", "event_type")
)
