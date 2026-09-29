from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "right")
    .select(
        F.col("i.last_restock"),
        F.col("i.product_id"),
        F.col("p.brand"),
        F.col("s.country"),
    )
    .orderBy(F.col("last_restock").desc())
)
