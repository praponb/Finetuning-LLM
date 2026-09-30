from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .filter(F.col("i.last_restock").between("2024-07-01", "2025-03-13") | (F.col("p.brand") == "Initech"))
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("i.store_id"),
        F.col("i.quantity"),
        F.col("p.launch_date"),
    )
    .orderBy("store_id", "id")
    .limit(10)
)
