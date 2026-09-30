from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") > 2.2) & (F.col("rating") <= 1.8))
    .groupBy(
        F.when(F.col("rating") >= 4.6, "high").when(F.col("rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.round(F.avg("rating"), 2).alias("avg_rating"))
    .orderBy("rating_bucket")
)
