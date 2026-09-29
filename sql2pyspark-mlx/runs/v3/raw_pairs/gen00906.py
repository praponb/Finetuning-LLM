from pyspark.sql import functions as F

inventory_by_product = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.max("quantity").alias("max_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(inventory_by_product.alias("ibp"), F.col("p.id") == F.col("ibp.product_id"), "left")
    .filter(F.col("ibp.max_quantity") >= 220)
    .select(F.col("p.stock_qty"), F.col("ibp.avg_quantity"), F.col("ibp.max_quantity"))
    .orderBy("avg_quantity")
)
