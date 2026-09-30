from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "outer")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("p.product_name"),
        F.col("s.supplier_name"),
        F.col("s.country"),
        F.substring(F.col("s.supplier_name"), 1, 4).alias("supplier_name_prefix"),
    )
    .orderBy(F.col("id").desc())
)
