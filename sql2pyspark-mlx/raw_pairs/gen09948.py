from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("p.brand"),
        F.col("s.supplier_name"),
        F.col("s.country"),
    )
)
