from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.year(F.col("s.opened_date")) >= 2025)
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("s.city"),
        F.col("s.store_name"),
        F.col("p.price"),
        F.col("p.stock_qty"),
    )
)
