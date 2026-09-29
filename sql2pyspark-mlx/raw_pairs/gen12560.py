from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 423, "high").when(F.col("duration_sec") >= 184, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("page") == "/product", 1).otherwise(0)).alias("product_count"),
    )
    .orderBy(F.col("product_count").desc(), "duration_sec_bucket")
    .limit(10)
)
