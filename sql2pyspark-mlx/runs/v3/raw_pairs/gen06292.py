from pyspark.sql import functions as F

filtered_products = df_products.filter(F.col("cost") >= 260)

products_stats = (
    filtered_products
    .groupBy("category")
    .agg(F.count("stock_qty").alias("stock_qty_count"))
)

result = products_stats.filter(F.col("stock_qty_count") > 9)
