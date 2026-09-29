from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.7, "high").when(F.col("rating") >= 2.3, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.avg("rating").alias("avg_rating"),
        F.max("rating").alias("max_rating"),
        F.min("rating").alias("min_rating"),
    )
    .filter(F.col("avg_rating") >= 3.6)
    .orderBy(F.col("min_rating").desc())
)
