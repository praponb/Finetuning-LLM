from pyspark.sql import functions as F

inventory_by_product = (
    df_inventory
    .filter(F.col("last_restock").between("2023-12-01", "2025-03-01"))
    .groupBy("product_id")
    .agg(F.count("quantity").alias("quantity_count"))
)

result = (
    df_products.alias("p")
    .join(inventory_by_product.alias("ibp"), F.col("p.id") == F.col("ibp.product_id"), "inner")
    .filter(F.col("ibp.quantity_count") >= 9)
    .select(
        F.col("p.id"),
        F.col("p.supplier_id"),
        F.col("p.price"),
        F.col("ibp.quantity_count"),
    )
)
