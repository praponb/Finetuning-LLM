from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.filter(F.col("price") <= 1010).alias("p"), F.col("i.product_id") == F.col("p.id"), "left_semi")
    .select(F.col("i.id"), F.col("i.store_id"))
    .orderBy("id", "store_id")
)
