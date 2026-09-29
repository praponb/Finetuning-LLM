from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "US")
    .groupBy(
        "city",
        F.when(F.col("rating") >= 3.7, "high").when(F.col("rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"))
    .filter(F.col("singapore_count") >= 9)
    .select("city", "rating_bucket", "singapore_count")
)
