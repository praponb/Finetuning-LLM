from pyspark.sql import functions as F

inventory_by_product = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.min("last_restock").alias("min_last_restock"),
    )
)

result = (
    df_products.alias("p")
    .join(inventory_by_product.alias("ibp"), F.col("p.id") == F.col("ibp.product_id"), "left")
    .select(
        F.col("p.cost"),
        F.col("p.category"),
        F.col("ibp.avg_quantity"),
        F.col("ibp.min_last_restock"),
    )
    .orderBy("cost")
)
