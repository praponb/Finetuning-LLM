from pyspark.sql import functions as F

inventory_by_product = (
    df_inventory
    .filter(F.month("last_restock").isin([1, 4, 5]))
    .groupBy("product_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)

result = (
    df_products.alias("p")
    .join(inventory_by_product.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .filter(F.col("t.quantity_range") >= 59)
    .select(
        F.col("p.id"),
        F.col("p.brand"),
        F.col("p.supplier_id"),
        F.col("t.min_quantity"),
        F.col("t.quantity_range"),
    )
)
