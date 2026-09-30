from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_type") == "page_view") & F.col("duration_sec").between(145, 440) & (F.col("event_time") < "2024-07-31"))
    .groupBy("event_type")
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
    )
    .orderBy(F.col("event_type").desc(), "max_duration_sec")
)
