from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.filter(F.col("store_id").isin([4, 5])).alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left_anti")
    .select(F.col("p.id"), F.col("p.category"), F.col("p.brand"), F.col("p.stock_qty"))
)
