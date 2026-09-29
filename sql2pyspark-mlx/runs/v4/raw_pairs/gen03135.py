from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") < 3.8)
    .groupBy(
        F.when(F.col("rating") >= 3.9, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum("rating").alias("total_rating"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
        F.sum(F.when(F.col("city") == "London", F.col("rating")).otherwise(0)).alias("london_rating"),
    )
    .orderBy(F.col("total_rating").desc(), "stddev_rating")
)
