from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "right")
    .select(
        F.col("i.last_restock"),
        F.col("p.brand"),
        F.col("p.category"),
        F.col("s.city"),
        F.col("s.supplier_name"),
    )
    .orderBy("supplier_name")
)
