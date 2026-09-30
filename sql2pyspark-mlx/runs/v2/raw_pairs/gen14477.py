from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.7, "high").when(F.col("rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(F.sum(F.when(F.col("country") == "US", 1).otherwise(0)).alias("us_count"))
    .filter(F.col("us_count") >= 80)
    .select("rating_bucket", "city", "us_count")
    .orderBy("city", "rating_bucket")
    .limit(10)
)
