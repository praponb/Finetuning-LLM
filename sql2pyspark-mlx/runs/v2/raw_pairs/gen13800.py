from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.5, "high").when(F.col("rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.avg("rating").alias("avg_rating"),
        F.min("rating").alias("min_rating"),
    )
    .filter(F.col("avg_rating") < 3.6)
    .select("rating_bucket", "avg_rating", "min_rating")
)
