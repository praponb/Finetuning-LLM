from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0)).alias("search_count"),
    )
    .filter(F.col("max_duration_sec") > 140)
)
