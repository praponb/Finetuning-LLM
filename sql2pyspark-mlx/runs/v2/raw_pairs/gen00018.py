from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("i.last_restock") >= "2023-06-28")
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("p.brand"),
        F.col("p.stock_qty"),
        F.col("s.store_name"),
        F.col("s.city"),
        F.last_day(F.col("p.launch_date")).alias("launch_date_month_end"),
    )
)
