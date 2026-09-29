from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.filter(F.array_contains("tags", "premium")).alias("p"), F.col("oi.product_id") == F.col("p.id"), "left_semi")
    .select(F.col("oi.id"), F.col("oi.quantity"), F.col("oi.product_id"))
)
