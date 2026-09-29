from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.1, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.avg("rating").alias("avg_rating"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
        F.count("*").alias("cnt"),
    )
    .orderBy("cnt")
)
