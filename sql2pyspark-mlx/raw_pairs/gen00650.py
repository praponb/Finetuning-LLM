from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(F.count(F.when(F.col("event_type") == "search", 1)).alias("search_count"))
    .orderBy(F.col("search_count").desc(), F.col("event_type").desc())
    .limit(1)
)
