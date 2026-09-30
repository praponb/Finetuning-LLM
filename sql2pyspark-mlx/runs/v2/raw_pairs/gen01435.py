from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.cost") > 560)
    .select(
        F.col("i.product_id"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.col("p.supplier_id"),
        (F.col("p.price") + F.col("p.stock_qty")).alias("price_plus_stock_qty"),
    )
)
