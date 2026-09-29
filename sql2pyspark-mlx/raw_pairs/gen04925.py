from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.size(F.col("p.tags")) > 1)
    .select(F.col("oi.id"), F.col("oi.order_id"), F.col("p.cost"))
)
