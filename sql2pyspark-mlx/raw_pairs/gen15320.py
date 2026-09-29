from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.price") >= 430)
    .select(
        F.col("i.last_restock"),
        F.col("i.product_id"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.col("p.stock_qty"),
        F.col("p.product_name"),
        F.datediff(F.current_date(), F.col("s.opened_date")).alias("days_since_opened_date"),
    )
)
