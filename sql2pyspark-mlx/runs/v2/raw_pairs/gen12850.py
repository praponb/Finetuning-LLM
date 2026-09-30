from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "left")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "right")
    .filter(F.month(F.col("p.launch_date")).isin([6, 10, 12]))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("s.region"),
        F.col("p.price"),
        F.col("p.stock_qty"),
    )
)
