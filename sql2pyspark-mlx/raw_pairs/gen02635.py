from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 16634, "high").when(F.col("sqft") >= 6672, "mid").otherwise("low").alias("sqft_bucket"),
        F.year("opened_date").alias("opened_year"),
    )
    .agg(
        F.max("sqft").alias("max_sqft"),
        F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0)).alias("tokyo_count"),
    )
    .orderBy("tokyo_count")
)
