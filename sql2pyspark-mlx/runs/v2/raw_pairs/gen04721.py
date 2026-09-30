from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("i.store_id").isin([3, 4, 5]))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("i.store_id"),
        F.col("p.product_name"),
        F.col("p.brand"),
        F.col("s.supplier_name"),
        F.col("s.city"),
    )
    .orderBy("city")
)
