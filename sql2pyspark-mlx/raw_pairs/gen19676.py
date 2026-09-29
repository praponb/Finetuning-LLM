from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 13512, "high").when(F.col("sqft") >= 8276, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.count("*").alias("store_count"),
    )
    .filter(F.col("store_count") > 4)
    .orderBy(F.col("store_count").desc())
)
