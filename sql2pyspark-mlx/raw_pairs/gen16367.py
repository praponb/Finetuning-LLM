from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec").between(248, 278) & (F.col("page") == "/checkout")) | (F.col("duration_sec") < 376))
    .groupBy(
        F.when(F.col("duration_sec") >= 464, "high").when(F.col("duration_sec") >= 73, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("page") == "/search", F.col("duration_sec")).otherwise(0)).alias("search_duration_sec"),
    )
    .orderBy("search_duration_sec", "duration_sec_bucket")
)
