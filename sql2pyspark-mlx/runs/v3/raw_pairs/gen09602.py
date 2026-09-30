from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        "city",
        F.when(F.col("sqft") >= 17593, "high").when(F.col("sqft") >= 3702, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.sum("sqft").alias("total_sqft"),
        F.avg("sqft").alias("avg_sqft"),
        F.sum(F.when(F.col("city") == "London", 1).otherwise(0)).alias("london_count"),
    )
    .filter(F.col("london_count") < 39)
    .drop("london_count")
    .orderBy(F.col("total_sqft").desc(), F.col("avg_sqft").desc())
)
