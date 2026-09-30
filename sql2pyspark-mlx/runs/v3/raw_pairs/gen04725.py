from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.6, "high").when(F.col("rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.round(F.stddev("rating"), 2).alias("stddev_rating"))
    .filter(F.col("stddev_rating") > 64)
    .orderBy(F.col("rating_bucket").desc(), F.col("stddev_rating").desc())
)
