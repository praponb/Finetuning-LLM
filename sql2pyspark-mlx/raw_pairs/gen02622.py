from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").isNull())
    .rollup("page", "event_type")
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.count(F.when(F.col("event_type") == "page_view", 1)).alias("page_view_count"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
)
