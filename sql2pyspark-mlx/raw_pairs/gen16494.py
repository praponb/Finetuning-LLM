from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.0, "high").when(F.col("rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.countDistinct("rating").alias("unique_citys"),
    )
    .filter(F.col("unique_citys") < 10)
    .select("rating_bucket", "avg_rating", "unique_citys")
)
