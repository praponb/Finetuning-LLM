from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left_anti")
    .select(F.col("p.id"), F.col("p.price"), F.col("p.stock_qty"), F.col("p.cost"))
)
