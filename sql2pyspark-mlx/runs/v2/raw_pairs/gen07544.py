from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("i.last_restock").between("2023-04-01", "2025-06-30"))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("s.sqft"),
        F.col("s.store_name"),
        F.col("p.stock_qty"),
    )
    .orderBy(F.col("stock_qty").desc())
)
