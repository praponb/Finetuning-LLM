from pyspark.sql import functions as F

inventory_by_product = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.sum("quantity").alias("total_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(inventory_by_product.alias("ibp"), F.col("p.id") == F.col("ibp.product_id"), "inner")
    .filter(F.col("ibp.total_quantity") >= 730)
    .select(
        F.col("p.product_name"),
        F.col("ibp.min_quantity"),
        F.col("ibp.total_quantity"),
    )
    .orderBy("product_name")
)
