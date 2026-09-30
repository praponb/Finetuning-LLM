from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_type") == "page_view") & F.col("duration_sec").between(144, 445) & F.col("duration_sec").between(280, 425))
    .agg(
        F.min("duration_sec").alias("min_duration_sec"),
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
        F.countDistinct("duration_sec").alias("unique_event_types"),
    )
    .orderBy("event_type_values")
)
