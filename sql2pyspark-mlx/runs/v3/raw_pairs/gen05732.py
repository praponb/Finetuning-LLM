from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 17652, "high").when(F.col("sqft") >= 6955, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.count(F.when(F.col("region") == "East", 1)).alias("east_count"),
    )
)
