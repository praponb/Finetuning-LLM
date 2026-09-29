from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.3, "high").when(F.col("rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.min("rating").alias("min_rating"),
        F.avg("rating").alias("avg_rating"),
    )
    .filter(F.col("avg_rating") >= 2.9)
    .select("rating_bucket", "min_rating", "avg_rating")
    .orderBy(F.col("rating_bucket").desc())
    .limit(3)
)
