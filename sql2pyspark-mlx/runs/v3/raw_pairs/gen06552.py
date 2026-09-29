from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").isNull())
    .groupBy(F.to_date("event_time").alias("event_day"), "event_type")
    .agg(F.count(F.when(F.col("event_type") == "search", 1)).alias("search_count"))
    .filter(F.col("search_count") < 9)
    .orderBy("search_count")
)
