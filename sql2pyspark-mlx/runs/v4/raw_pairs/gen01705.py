from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        "country",
        F.when(F.col("rating") >= 4.4, "high").when(F.col("rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_count"),
    )
    .filter(F.col("bangkok_count") >= 1)
    .orderBy(F.col("bangkok_count").desc())
)
