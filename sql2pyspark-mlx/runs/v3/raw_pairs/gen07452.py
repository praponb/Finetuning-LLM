from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.3, "high").when(F.col("rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "Singapore", 1).otherwise(0)).alias("singapore_count"),
    )
    .filter(F.col("singapore_count") > 36)
    .orderBy("rating_bucket")
    .limit(5)
)
