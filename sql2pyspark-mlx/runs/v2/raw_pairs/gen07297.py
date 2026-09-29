from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        "page",
        F.when(F.col("duration_sec") >= 526, "high").when(F.col("duration_sec") >= 143, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.min("duration_sec").alias("min_duration_sec"),
        F.count("*").alias("row_count"),
    )
)
