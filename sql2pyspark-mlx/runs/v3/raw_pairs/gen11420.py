from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("p.launch_date").between("2024-10-01", "2025-07-01"))
    .groupBy(F.col("p.supplier_id"))
    .agg(F.max(F.col("i.quantity")).alias("max_quantity"))
)
