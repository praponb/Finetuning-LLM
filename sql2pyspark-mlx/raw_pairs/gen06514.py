from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.size(F.col("p.tags")) >= 2)
    .groupBy(F.col("p.category"))
    .agg(F.min(F.col("i.last_restock")).alias("min_last_restock"))
    .orderBy("category", F.col("min_last_restock").desc())
)
