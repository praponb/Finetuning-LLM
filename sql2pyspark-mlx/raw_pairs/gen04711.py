from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.filter(F.col("store_id") == 5).alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left_semi")
    .select(F.col("p.id"), F.col("p.stock_qty"))
    .orderBy(F.col("stock_qty").desc())
)
