from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") < 2.7)
    .groupBy(
        F.when(F.col("rating") >= 4.3, "high").when(F.col("rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(
        F.count("*").alias("num_suppliers"),
        F.sum(F.when(F.col("city") == "Singapore", 1).otherwise(0)).alias("singapore_count"),
    )
    .filter(F.col("num_suppliers") < 6)
    .orderBy(F.col("singapore_count").desc())
)
