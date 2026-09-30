from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull())
    .groupBy(
        F.when(F.col("rating") >= 3.7, "high").when(F.col("rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.avg("rating").alias("avg_rating"),
        F.sum("rating").alias("total_rating"),
    )
    .filter(F.col("avg_rating") > 3.2)
    .select("rating_bucket", "avg_rating", "total_rating")
)
