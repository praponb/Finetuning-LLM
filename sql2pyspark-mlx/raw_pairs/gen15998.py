from pyspark.sql import functions as F

inventory_by_product = (
    df_inventory
    .groupBy("product_id")
    .agg(F.countDistinct("quantity").alias("unique_stores"))
)

result = (
    df_products.alias("p")
    .join(inventory_by_product.alias("I"), F.col("p.id") == F.col("I.product_id"), "inner")
    .select(F.col("p.id"), F.col("p.brand"), F.col("I.unique_stores"))
    .orderBy(F.col("unique_stores").desc())
)
