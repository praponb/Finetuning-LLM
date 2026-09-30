from pyspark.sql import functions as F

src = (
    df_web_events
    .filter(((F.col("event_time") < "2025-10-01") & F.col("session_id").isin([24, 25, 33])) | F.col("session_id").isin([1, 4, 34]))
    .withColumn("page_lower", F.lower("page"))
)

web_events_stats = (
    src
    .groupBy(F.to_date("event_time").alias("event_day"))
    .agg(
        F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("page_view_count"),
    )
)

result = web_events_stats.orderBy(F.col("page_view_count").desc())
