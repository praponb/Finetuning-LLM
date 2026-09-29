from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "UK") & F.col("supplier_name").isNull())
    .groupBy(
        F.when(F.col("rating") >= 4.5, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count("rating").alias("rating_count"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
    )
    .filter(F.col("stddev_rating") >= 30)
    .orderBy("rating_bucket", F.col("london_count").desc())
    .limit(10)
)
