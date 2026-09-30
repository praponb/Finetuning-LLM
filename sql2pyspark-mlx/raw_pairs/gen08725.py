from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "left")
    .filter(F.col("p.price") <= 740)
    .select(
        F.col("i.last_restock"),
        F.col("p.launch_date"),
        F.col("p.stock_qty"),
        F.col("s.city"),
        F.col("s.sqft"),
    )
)
