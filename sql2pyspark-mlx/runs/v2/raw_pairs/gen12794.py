from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") != "DE")
    .groupBy(
        F.when(F.col("rating") >= 4.2, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
    )
)
