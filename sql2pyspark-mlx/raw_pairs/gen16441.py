from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .filter(F.col("last_restock") <= "2024-11-01")
    .groupBy("product_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.countDistinct("quantity").alias("unique_stores"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .select(F.col("p.supplier_id"), F.col("t.quantity_count"), F.col("t.unique_stores"))
)
