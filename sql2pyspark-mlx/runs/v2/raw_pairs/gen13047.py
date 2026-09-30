from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(F.sum("stock_qty").alias("total_stock_qty"))
    .orderBy("brand", F.col("total_stock_qty").desc())
)
