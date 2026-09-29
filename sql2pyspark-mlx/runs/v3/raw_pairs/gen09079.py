from pyspark.sql import functions as F

filtered_suppliers = df_suppliers.filter((F.col("rating") <= 2.8) | (F.col("country") == "UK"))

suppliers_stats = (
    filtered_suppliers
    .groupBy(
        "city",
        F.when(F.col("rating") >= 4.4, "high").when(F.col("rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("country") == "UK", 1).otherwise(0)).alias("uk_count"),
        F.count("rating").alias("rating_count"),
    )
)

result = suppliers_stats.select("city", "rating_bucket", "uk_count", "rating_count")
