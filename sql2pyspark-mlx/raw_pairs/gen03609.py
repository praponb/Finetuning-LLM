from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 16016, "high").when(F.col("sqft") >= 7368, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.countDistinct("sqft").alias("unique_regions"),
        F.round(((100 * F.sum(F.when(F.col("region") == "East", 1).otherwise(0))) / F.count("*")), 1).alias("pct_east"),
    )
    .filter(F.col("unique_regions") >= 3)
    .orderBy("unique_regions", F.col("sqft_bucket").desc())
)
