from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.8, "high").when(F.col("rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.countDistinct("rating").alias("unique_countrys"),
        F.sum("rating").alias("total_rating"),
    )
    .orderBy("unique_countrys", "rating_bucket")
    .limit(5)
)
