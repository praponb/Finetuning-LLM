from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("i.quantity").between(146, 168) | (F.col("i.last_restock") >= "2024-02-01"))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("p.supplier_id"),
        F.col("p.launch_date"),
        F.col("s.country"),
        F.col("s.city"),
    )
)
