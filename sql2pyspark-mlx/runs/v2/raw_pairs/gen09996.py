from pyspark.sql import functions as F

inventory_by_product = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3, 5, 6]))
    .groupBy("product_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.count("*").alias("total_count"),
    )
)

result = (
    df_products.alias("p")
    .join(inventory_by_product.alias("ibp"), F.col("p.id") == F.col("ibp.product_id"), "inner")
    .filter(F.col("ibp.avg_quantity") >= 110)
    .select(
        F.col("p.launch_date"),
        F.col("p.product_name"),
        F.col("ibp.avg_quantity"),
        F.col("ibp.total_count"),
    )
    .orderBy("total_count")
)
