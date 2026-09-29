from pyspark.sql import functions as F

inventory_by_product = (
    df_inventory
    .filter(F.col("product_id") == 6)
    .groupBy("product_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = (
    df_products.alias("p")
    .join(inventory_by_product.alias("ibp"), F.col("p.id") == F.col("ibp.product_id"), "left")
    .filter(F.col("ibp.avg_quantity") > 130)
    .select(
        F.col("p.id"),
        F.col("p.supplier_id"),
        F.col("p.launch_date"),
        F.col("ibp.avg_quantity"),
    )
    .orderBy(F.col("avg_quantity").desc())
)
