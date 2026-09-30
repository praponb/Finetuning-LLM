from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.5, "high").when(F.col("rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.avg("rating").alias("avg_rating"))
    .filter(F.col("avg_rating") >= 3.3)
    .select("rating_bucket", "avg_rating")
    .orderBy("avg_rating")
)
