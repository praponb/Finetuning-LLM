from pyspark.sql import functions as F

inventory_by_product = (
    df_inventory
    .filter(F.month("last_restock").isin([3, 4, 8]))
    .groupBy("product_id")
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = (
    df_products.alias("p")
    .join(inventory_by_product.alias("ibp"), F.col("p.id") == F.col("ibp.product_id"), "inner")
    .select(F.col("p.supplier_id"), F.col("p.launch_date"), F.col("ibp.total_quantity"))
)
