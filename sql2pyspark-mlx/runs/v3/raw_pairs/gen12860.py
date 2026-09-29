from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("p.product_name"),
        F.col("s.city"),
    )
    .orderBy(F.col("id").desc())
)
