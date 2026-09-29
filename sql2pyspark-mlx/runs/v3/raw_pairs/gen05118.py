from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.4, "high").when(F.col("rating") >= 2.3, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count("rating").alias("rating_count"),
        F.round(((100 * F.sum(F.when(F.col("country") == "DE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_de"),
        F.sum("rating").alias("total_rating"),
    )
    .filter(F.col("total_rating") > 16)
    .drop("total_rating")
    .orderBy("rating_count", "rating_bucket")
    .limit(1)
)
