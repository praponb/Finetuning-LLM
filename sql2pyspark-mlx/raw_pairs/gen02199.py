from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("P%") & (F.col("country") == "DE"))
    .groupBy(
        F.when(F.col("rating") >= 4.0, "high").when(F.col("rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.max("rating").alias("max_rating"),
        F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0)).alias("tokyo_count"),
        F.count("*").alias("total_count"),
    )
    .orderBy("rating_bucket")
)
