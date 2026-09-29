from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(F.col("p.supplier_id"), F.col("p.brand"))
    .agg(F.min(F.col("i.last_restock")).alias("min_last_restock"))
    .orderBy("min_last_restock")
)
