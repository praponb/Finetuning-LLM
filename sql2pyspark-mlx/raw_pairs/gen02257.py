from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 15479, "high").when(F.col("sqft") >= 6028, "mid").otherwise("low").alias("sqft_bucket"),
        "city",
    )
    .agg(
        F.min("sqft").alias("min_sqft"),
        F.count("sqft").alias("sqft_count"),
    )
    .orderBy("city", "sqft_bucket")
    .limit(50)
)
