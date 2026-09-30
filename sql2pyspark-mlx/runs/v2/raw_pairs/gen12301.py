from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.9, "high").when(F.col("rating") >= 2.3, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.min("rating").alias("min_rating"),
        F.avg("rating").alias("avg_rating"),
        F.count("*").alias("supplier_count"),
    )
    .filter(F.col("min_rating") >= 2.1)
)
