from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .filter(F.col("i.quantity") != 231)
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.last_restock"),
        F.col("p.stock_qty"),
        F.col("p.category"),
        F.col("s.store_name"),
        F.col("s.sqft"),
        F.initcap(F.trim(F.col("p.brand"))).alias("brand_title"),
    )
)
