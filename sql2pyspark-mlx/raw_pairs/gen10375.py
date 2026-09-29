from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "outer")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.brand") == "Initech")
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("i.quantity"),
        F.col("s.sqft"),
        F.col("s.region"),
        F.col("p.category"),
        F.col("p.stock_qty"),
        F.initcap(F.trim(F.col("s.store_name"))).alias("store_name_title"),
    )
)
