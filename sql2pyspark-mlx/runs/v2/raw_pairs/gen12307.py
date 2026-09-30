from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "right")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("i.last_restock").between("2024-10-01", "2025-06-24"))
    .select(F.col("i.store_id"), F.col("p.cost"), F.col("s.supplier_name"))
    .orderBy(F.col("cost").desc(), F.col("store_id").desc())
)
