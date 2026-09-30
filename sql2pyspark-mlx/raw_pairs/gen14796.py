from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter((F.col("p.category") == "Home") & F.col("s.supplier_name").isNotNull())
    .select(F.col("i.id"), F.col("i.store_id"), F.col("p.price"), F.col("s.country"))
)
