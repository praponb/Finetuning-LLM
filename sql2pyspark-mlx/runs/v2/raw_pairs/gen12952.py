from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "New York")
    .groupBy(
        F.when(F.col("rating") >= 3.9, "high").when(F.col("rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.count(F.when(F.col("country") == "US", 1)).alias("us_count"))
    .filter(F.col("us_count") >= 8)
    .orderBy("rating_bucket", F.col("us_count").desc())
)
