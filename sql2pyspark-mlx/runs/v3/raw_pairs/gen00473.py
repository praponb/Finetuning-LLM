from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .select(
        F.col("i.store_id"),
        F.col("p.supplier_id"),
        F.col("s.supplier_name"),
        F.ceil(F.col("p.price")).alias("price_whole"),
    )
    .orderBy(F.col("store_id").desc())
)
