from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        "country",
        F.when(F.col("rating") >= 4.5, "high").when(F.col("rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "Singapore", 1).otherwise(0)).alias("singapore_count"),
    )
    .orderBy("rating_bucket", "country")
)
