from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.1, "high").when(F.col("rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(
        F.min("rating").alias("min_rating"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"),
    )
    .orderBy(F.col("min_rating").desc(), "rating_bucket", "city")
    .limit(3)
)
