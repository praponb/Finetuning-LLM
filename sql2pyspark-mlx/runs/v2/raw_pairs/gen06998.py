from pyspark.sql import functions as F

result = (
    df_products
    .agg(F.count("stock_qty").alias("stock_qty_count"))
    .orderBy(F.col("stock_qty_count").desc())
)
