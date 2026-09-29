from pyspark.sql import functions as F

recent_products = (
    df_products
    .filter((F.col("stock_qty") == 283) & (F.col("cost") >= 190) & F.col("cost").isNotNull())
)

products_stats = (
    recent_products
    .groupBy(
        F.when(F.col("stock_qty") >= 324, "high").when(F.col("stock_qty") >= 64, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("category") == "Fashion", 1).otherwise(0)).alias("fashion_count"),
    )
)

result = (
    products_stats
    .filter(F.col("fashion_count") >= 67)
    .orderBy(F.col("fashion_count").desc(), "stock_qty_bucket")
    .limit(10)
)
