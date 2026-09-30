from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["London", "Tokyo"]) & (F.col("rating") >= 3.4) & F.col("country").isin(["US", "SG", "DE"]))
    .groupBy(
        F.when(F.col("city").isin(["Berlin", "London"]), "priority").otherwise("other").alias("city_group"),
        F.when(F.col("rating") >= 4.3, "high").when(F.col("rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.round(F.stddev("rating"), 2).alias("stddev_rating"))
    .filter(F.col("stddev_rating") >= 27)
    .orderBy("city_group", "rating_bucket")
    .limit(50)
)
