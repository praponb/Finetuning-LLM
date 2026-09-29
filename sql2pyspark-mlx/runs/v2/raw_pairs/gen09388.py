from pyspark.sql import functions as F

inventory_by_product = (
    df_inventory
    .groupBy("product_id")
    .agg(F.count("quantity").alias("quantity_count"))
)

result = (
    df_products.alias("p")
    .join(inventory_by_product.alias("ibp"), F.col("p.id") == F.col("ibp.product_id"), "inner")
    .filter(F.col("ibp.quantity_count") >= 7)
    .select(F.col("p.product_name"), F.col("ibp.quantity_count"))
    .orderBy(F.col("quantity_count").desc(), "product_name")
)
