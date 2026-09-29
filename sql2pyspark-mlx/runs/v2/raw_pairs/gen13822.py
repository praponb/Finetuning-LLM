from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .filter(F.col("quantity").between(4, 8))
    .groupBy("product_id")
    .agg(F.countDistinct("quantity").alias("unique_products"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.unique_products") > 4)
    .select(F.col("p.product_name"), F.col("p.stock_qty"), F.col("t.unique_products"))
    .orderBy(F.col("stock_qty").desc())
)
