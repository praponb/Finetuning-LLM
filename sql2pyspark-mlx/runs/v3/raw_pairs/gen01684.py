from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.9, "high").when(F.col("rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_count"))
    .filter(F.col("berlin_count") >= 7)
)
