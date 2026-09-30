from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("oi.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.price") <= 820)
    .select(F.col("p.stock_qty"), F.col("oi.quantity"))
)
