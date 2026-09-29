from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 14338, "high").when(F.col("sqft") >= 4617, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.min("sqft").alias("min_sqft"),
        F.sum("sqft").alias("total_sqft"),
        F.count("sqft").alias("sqft_count"),
    )
    .filter(F.col("sqft_count") > 4)
    .drop("sqft_count")
    .orderBy(F.col("min_sqft").desc())
)
