from pyspark.sql import functions as F

inventory_by_product = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.min("last_restock").alias("first_last_restock"),
        F.max("quantity").alias("max_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(inventory_by_product.alias("ibp"), F.col("p.id") == F.col("ibp.product_id"), "inner")
    .select(
        F.col("p.price"),
        F.col("p.cost"),
        F.col("ibp.first_last_restock"),
        F.col("ibp.max_quantity"),
    )
    .orderBy("price")
)
