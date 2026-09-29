from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("i.product_id").isin([2, 3, 9]) & (F.col("i.last_restock") > "2025-07-01"))
    .select(F.col("i.quantity"), F.col("p.stock_qty"), F.col("s.sqft"), F.col("s.city"))
)
