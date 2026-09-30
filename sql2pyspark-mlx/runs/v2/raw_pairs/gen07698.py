from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("Pac%") & (F.col("rating") >= 2.4))
    .groupBy(
        F.when(F.col("rating") >= 4.4, "high").when(F.col("rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.min("rating").alias("min_rating"),
        F.avg("rating").alias("avg_rating"),
    )
    .filter(F.col("avg_rating") >= 2.2)
)
