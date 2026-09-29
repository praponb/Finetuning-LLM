from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") <= 4.1)
    .groupBy(
        F.when(F.col("rating") >= 3.6, "high").when(F.col("rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_count"),
    )
    .filter(F.col("berlin_count") < 6)
    .orderBy("berlin_count", F.col("avg_rating").desc())
)
