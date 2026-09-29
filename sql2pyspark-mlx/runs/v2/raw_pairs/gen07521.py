from pyspark.sql import functions as F

inventory_by_product = (
    df_inventory
    .filter(F.year("last_restock") == 2024)
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.avg("quantity").alias("avg_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(inventory_by_product.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.total_quantity") > 540)
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("t.total_quantity"),
        F.col("t.avg_quantity"),
    )
    .orderBy(F.col("id").desc())
)
