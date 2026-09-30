from pyspark.sql import functions as F

recent_products = (
    df_products
    .filter(F.col("category").isin(["Books", "Home", "Fashion"]) & ((F.col("brand") == "Umbrella") | (F.col("cost") <= 720)))
)

cost_bucket_summary = (
    recent_products
    .groupBy(
        F.when(F.col("cost") >= 650, "high").when(F.col("cost") >= 270, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.count(F.when(F.col("brand") == "Acme", 1)).alias("acme_count"),
        F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_count"),
    )
)

result = cost_bucket_summary.orderBy(F.col("fashion_count").desc())
