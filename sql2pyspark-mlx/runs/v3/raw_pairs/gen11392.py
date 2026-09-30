from pyspark.sql import functions as F

inventory_by_product = (
    df_inventory
    .groupBy("product_id")
    .agg(F.count("quantity").alias("quantity_count"))
)

result = (
    df_products.alias("p")
    .join(inventory_by_product.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .filter(F.col("t.quantity_count") >= 7)
    .select(
        F.col("p.id"),
        F.col("p.category"),
        F.col("p.supplier_id"),
        F.col("t.quantity_count"),
    )
    .orderBy(F.col("id").desc())
)
