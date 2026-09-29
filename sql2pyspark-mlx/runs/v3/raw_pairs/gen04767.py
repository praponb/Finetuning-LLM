from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        "city",
        F.when(F.col("sqft") >= 16329, "high").when(F.col("sqft") >= 8146, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"),
        F.min("sqft").alias("min_sqft"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
    )
    .orderBy(F.col("tokyo_count").desc())
)
