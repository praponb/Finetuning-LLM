from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.filter(F.col("supplier_id").isin([26, 38])).alias("p"), F.col("i.product_id") == F.col("p.id"), "left_semi")
    .filter(F.col("i.product_id").isin([3, 8, 9, 10]))
    .select(F.col("i.id"), F.col("i.product_id"), F.col("i.store_id"))
)
