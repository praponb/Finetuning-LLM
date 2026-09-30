from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") != "US")
    .groupBy(
        F.when(F.col("rating") >= 4.1, "high").when(F.col("rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "Chiang Mai", 1).otherwise(0)).alias("chiang_mai_count"),
        (F.max("rating") - F.min("rating")).alias("rating_range"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
    )
)
