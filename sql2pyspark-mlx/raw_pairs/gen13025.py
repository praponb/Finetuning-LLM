from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter((F.col("p.category") == "Grocery") | (F.year(F.col("i.last_restock")) >= 2025))
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.last_restock"),
        F.col("p.price"),
        F.col("s.country"),
        F.col("s.city"),
    )
    .orderBy(F.col("price").desc(), "id")
    .limit(1)
)
