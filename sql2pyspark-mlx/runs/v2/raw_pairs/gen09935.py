from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.2, "high").when(F.col("rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"),
        F.sum(F.when(F.col("country") == "UK", F.col("rating")).otherwise(0)).alias("uk_rating"),
    )
    .orderBy("uk_rating", "tokyo_count")
)
