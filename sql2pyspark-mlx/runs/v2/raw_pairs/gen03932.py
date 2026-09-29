from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("Run%"))
    .groupBy("brand")
    .agg(F.count("stock_qty").alias("stock_qty_count"))
)
