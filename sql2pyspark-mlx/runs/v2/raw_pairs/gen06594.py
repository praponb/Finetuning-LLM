from pyspark.sql import functions as F

base = df_suppliers.filter(F.col("rating") >= 3.9)

rating_bucket_summary = (
    base
    .groupBy(
        F.when(F.col("rating") >= 3.5, "high").when(F.col("rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.round(F.avg("rating"), 2).alias("avg_rating"))
)

result = (
    rating_bucket_summary
    .filter(F.col("avg_rating") > 75)
    .select("rating_bucket", "avg_rating")
    .orderBy(F.col("avg_rating").desc())
)
