from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter((F.col("s.country") != "JP") & (F.year(F.col("i.last_restock")) >= 2024))
    .select(
        F.col("i.quantity"),
        F.col("p.product_name"),
        F.col("p.price"),
        F.col("s.supplier_name"),
        F.col("s.city"),
    )
    .orderBy("quantity", F.col("supplier_name").desc())
)
