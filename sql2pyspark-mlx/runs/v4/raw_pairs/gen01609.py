from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") <= 3.6) & (F.col("supplier_name").like("%Tokyo%") | F.col("supplier_name").like("G%")))
    .groupBy(
        F.when(F.col("rating") >= 3.4, "high").when(F.col("rating") >= 2.3, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "Berlin", 1).otherwise(0)).alias("berlin_count"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"),
    )
)
