from pyspark.sql import functions as F

inventory_by_product = (
    df_inventory
    .filter(F.col("product_id").isin([2, 5]))
    .groupBy("product_id")
    .agg(F.max("quantity").alias("max_quantity"))
)

result = (
    df_products.alias("p")
    .join(inventory_by_product.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .select(F.col("p.id"), F.col("p.product_name"), F.col("t.max_quantity"))
    .orderBy(F.col("max_quantity").desc(), "id")
    .limit(5)
)
