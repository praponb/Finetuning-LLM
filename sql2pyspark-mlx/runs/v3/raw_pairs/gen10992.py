from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("session_id", "event_type")
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("page_view_count"),
        F.count("duration_sec").alias("duration_sec_count"),
    )
    .filter(F.col("duration_sec_count") > 5)
    .orderBy(F.col("page_view_count").desc())
)
