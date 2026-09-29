from pyspark.sql import functions as F

inventory_by_product = (
    df_inventory
    .filter(F.col("store_id").isin([1, 5, 6]))
    .groupBy("product_id")
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        F.max("last_restock").alias("latest_last_restock"),
    )
)

result = (
    df_products.alias("p")
    .join(inventory_by_product.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.unique_products") >= 6)
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("t.unique_products"),
        F.col("t.latest_last_restock"),
    )
    .orderBy(F.col("unique_products").desc(), F.col("id").desc())
    .limit(5)
)
