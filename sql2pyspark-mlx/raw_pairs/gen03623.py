from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter((F.upper(F.trim(F.col("s.store_name"))) == "SIAM FLAGSHIP") & F.col("s.store_name").like("Sia%"))
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("i.last_restock"),
        F.col("p.stock_qty"),
        F.col("s.region"),
        F.col("s.store_name"),
        (F.col("p.stock_qty") * F.col("p.price")).alias("stock_qty_times_price"),
    )
    .orderBy("region")
)
