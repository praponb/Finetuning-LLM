from pyspark.sql import functions as F

inventory_by_product = (
    df_inventory
    .filter(F.col("last_restock") <= "2023-09-18")
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.count("*").alias("total_count"),
    )
)

result = (
    df_products.alias("p")
    .join(inventory_by_product.alias("I"), F.col("p.id") == F.col("I.product_id"), "inner")
    .filter(F.col("I.total_quantity") >= 960)
    .select(
        F.col("p.supplier_id"),
        F.col("p.price"),
        F.col("I.total_quantity"),
        F.col("I.total_count"),
    )
)
