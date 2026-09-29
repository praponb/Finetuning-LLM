from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") < 3.8)
    .groupBy(
        F.when(F.col("rating") >= 3.8, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("country") == "DE", 1).otherwise(0)).alias("de_count"),
        F.count("*").alias("total_count"),
        F.min("rating").alias("min_rating"),
    )
    .filter(F.col("de_count") > 76)
    .orderBy("rating_bucket")
)
