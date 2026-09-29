from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("page") == "/search") | (F.col("duration_sec") == 159))
    .groupBy(
        F.when(F.col("duration_sec") >= 528, "high").when(F.col("duration_sec") >= 144, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.count("*").alias("web_event_count"),
        F.sum(F.when(F.col("page") == "/search", 1).otherwise(0)).alias("search_count"),
    )
    .orderBy("web_event_count", F.col("search_count").desc())
)
