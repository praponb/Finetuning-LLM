from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(F.count("stock_qty").alias("stock_qty_count"))
    .orderBy("category", "stock_qty_count")
)
