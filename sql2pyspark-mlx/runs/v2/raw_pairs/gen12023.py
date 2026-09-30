from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.filter(F.col("unit_price") > 110).alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left_anti")
    .filter(F.col("p.stock_qty") != 259)
    .select(F.col("p.id"), F.col("p.category"), F.col("p.stock_qty"))
)
