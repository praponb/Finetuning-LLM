from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.year("event_time").alias("event_year"))
    .agg(
        F.min("duration_sec").alias("min_duration_sec"),
        F.sum(F.when(F.col("page") == "/search", 1).otherwise(0)).alias("search_count"),
        F.max("event_time").alias("last_event_time"),
    )
)
