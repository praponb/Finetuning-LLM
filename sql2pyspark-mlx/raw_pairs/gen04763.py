from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .filter(F.col("product_id") == 16)
    .groupBy("product_id")
    .agg(F.countDistinct("quantity").alias("unique_orders"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.unique_orders") >= 8)
    .select(F.col("p.stock_qty"), F.col("t.unique_orders"))
)
