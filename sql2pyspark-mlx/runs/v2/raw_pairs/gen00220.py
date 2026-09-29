from pyspark.sql import functions as F

valid_products = df_products.filter(F.size("tags") > 1)

products_stats = (
    valid_products
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("category") == "Home", 1).otherwise(0)).alias("home_count"),
        F.count("stock_qty").alias("stock_qty_count"),
    )
)

result = (
    products_stats
    .filter(F.col("home_count") >= 26)
    .select("supplier_id", "home_count", "stock_qty_count")
    .orderBy(F.col("supplier_id").desc())
)
