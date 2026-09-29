from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.filter(F.col("product_id").isNotNull()).alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left_semi")
    .select(F.col("p.id"), F.col("p.stock_qty"), F.col("p.price"), F.col("p.category"))
)
