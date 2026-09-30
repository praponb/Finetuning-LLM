from pyspark.sql import functions as F

inventory_by_product = (
    df_inventory
    .groupBy("product_id")
    .agg(F.max("last_restock").alias("last_last_restock"))
)

result = (
    df_products.alias("p")
    .join(inventory_by_product.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .select(F.col("p.id"), F.col("p.stock_qty"), F.col("t.last_last_restock"))
    .orderBy(F.col("last_last_restock").desc(), F.col("stock_qty").desc(), "id")
    .limit(5)
)
