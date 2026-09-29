from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("i.product_id").isin([5, 8]) & (F.col("s.city") != "Tokyo"))
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("i.store_id"),
        F.col("p.launch_date"),
        F.col("s.rating"),
        F.col("s.city"),
    )
)
