from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        "city",
        F.when(F.col("rating") >= 4.4, "high").when(F.col("rating") >= 1.8, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.avg("rating").alias("avg_rating"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
    )
    .filter(F.col("avg_rating") < 1.9)
    .orderBy("avg_rating")
)
