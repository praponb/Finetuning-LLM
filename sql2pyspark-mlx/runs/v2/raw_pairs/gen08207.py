from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("i.last_restock") > "2025-04-01")
    .groupBy(F.col("p.brand"))
    .agg(F.sum(F.col("i.quantity")).alias("total_quantity"))
)
