from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") >= "2023-10-01") | F.col("event_type").isin(["search", "purchase", "click"]))
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("page_view_count"),
    )
)
