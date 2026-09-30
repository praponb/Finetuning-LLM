from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        "page",
        F.when(F.col("duration_sec") >= 506, "high").when(F.col("duration_sec") >= 130, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.count("duration_sec").alias("duration_sec_count"),
        F.sum(F.when(F.col("page") == "/search", 1).otherwise(0)).alias("search_count"),
    )
    .filter(F.col("max_duration_sec") > 160)
)
