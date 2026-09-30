from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").isNotNull() & ((F.col("event_time") >= "2024-09-28") | (F.col("event_time") >= "2025-06-26")))
    .groupBy(F.date_trunc("MONTH", "event_time").alias("event_month"), "page")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
    )
)
