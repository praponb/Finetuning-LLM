from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 12260, "high").when(F.col("sqft") >= 4941, "mid").otherwise("low").alias("sqft_bucket"),
        "region",
    )
    .agg(
        F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0)).alias("tokyo_count"),
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.countDistinct("sqft").alias("unique_citys"),
    )
)
