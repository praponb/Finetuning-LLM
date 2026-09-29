from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("p.product_name"),
        F.col("p.category"),
        F.col("s.country"),
    )
    .orderBy("category", "last_restock")
)
