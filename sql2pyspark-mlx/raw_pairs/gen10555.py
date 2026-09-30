from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "Singapore")
    .groupBy(
        F.when(F.col("rating") >= 4.3, "high").when(F.col("rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.count(F.when(F.col("country") == "US", 1)).alias("us_count"))
    .filter(F.col("us_count") >= 2)
)
