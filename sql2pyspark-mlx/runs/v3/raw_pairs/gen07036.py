from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") >= "2023-12-23") & F.col("page").isin(["/search", "/cart"]))
    .groupBy(
        F.when(F.col("duration_sec") >= 492, "high").when(F.col("duration_sec") >= 165, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.count("*").alias("num_web_events"),
        F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("page_view_count"),
    )
    .filter(F.col("page_view_count") > 72)
    .drop("page_view_count")
    .orderBy("max_duration_sec")
)
