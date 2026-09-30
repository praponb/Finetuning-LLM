from pyspark.sql import functions as F

valid_products = df_products.filter(F.array_contains("tags", "clearance"))

cost_bucket_summary = (
    valid_products
    .groupBy(
        F.when(F.col("cost") >= 640, "high").when(F.col("cost") >= 320, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.count(F.when(F.col("brand") == "Acme", 1)).alias("acme_count"))
)

result = (
    cost_bucket_summary
    .filter(F.col("acme_count") >= 4)
    .orderBy(F.col("acme_count").desc())
)
