from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.filter(F.col("product_name").isNotNull()).alias("p"), F.col("i.product_id") == F.col("p.id"), "left_semi")
    .filter(F.col("i.last_restock") >= "2024-05-01")
    .select(F.col("i.id"), F.col("i.product_id"), F.col("i.quantity"))
    .orderBy(F.col("quantity").desc())
)
