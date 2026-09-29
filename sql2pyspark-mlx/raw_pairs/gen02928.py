from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 432, "high").when(F.col("duration_sec") >= 180, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("page") == "/cart", 1).otherwise(0)).alias("cart_count"),
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
    )
    .filter(F.col("cart_count") > 79)
    .orderBy("duration_sec_range")
)
