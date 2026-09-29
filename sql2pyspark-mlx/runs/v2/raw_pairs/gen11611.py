from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").between(3.7, 4.0))
    .groupBy(
        F.when(F.col("rating") >= 4.4, "high").when(F.col("rating") >= 2.3, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.countDistinct("rating").alias("unique_countrys"),
        F.sum(F.when(F.col("country") == "UK", F.col("rating")).otherwise(0)).alias("uk_rating"),
    )
)
