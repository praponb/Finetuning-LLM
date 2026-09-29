from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") > 3.9)
    .groupBy(
        F.when(F.col("rating") >= 4.3, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.sum("rating").alias("total_rating"))
    .filter(F.col("total_rating") > 12)
    .select("rating_bucket", "total_rating")
)
