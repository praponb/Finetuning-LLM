from pyspark.sql import functions as F

filtered_products = (
    df_products
    .filter((F.col("launch_date") >= "2024-09-01") | F.array_contains("tags", "clearance"))
)

products_stats = (
    filtered_products
    .groupBy("category")
    .agg(
        F.count("cost").alias("cost_count"),
        (F.max("stock_qty") - F.min("stock_qty")).alias("stock_qty_range"),
    )
)

result = products_stats.orderBy(F.col("category").desc())
