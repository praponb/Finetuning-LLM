from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(
        F.sum(F.when(F.col("page") == "/search", 1).otherwise(0)).alias("search_count"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
    .filter(F.col("search_count") < 50)
    .select("event_type", "search_count", "max_duration_sec")
    .orderBy("search_count", "event_type")
    .limit(5)
)
