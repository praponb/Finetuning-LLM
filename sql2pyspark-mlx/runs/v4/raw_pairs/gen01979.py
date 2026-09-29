from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.2, "high").when(F.col("rating") >= 1.8, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.countDistinct("rating").alias("unique_countrys"),
    )
)
