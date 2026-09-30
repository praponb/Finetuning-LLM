from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("s.city") == "Bangkok")
    .groupBy(F.col("p.supplier_id"))
    .agg(F.countDistinct(F.col("i.quantity")).alias("unique_stores"))
    .orderBy(F.col("supplier_id").desc())
)
