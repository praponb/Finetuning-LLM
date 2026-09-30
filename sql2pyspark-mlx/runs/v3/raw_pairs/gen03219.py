from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") < 3.9) | F.col("city").isin(["Singapore", "London"]))
    .groupBy(
        F.when(F.col("rating") >= 3.9, "high").when(F.col("rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum("rating").alias("total_rating"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
    )
    .filter(F.col("total_rating") >= 23)
    .orderBy("rating_bucket", "stddev_rating")
)
