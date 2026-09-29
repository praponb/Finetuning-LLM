from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .select(
        F.col("i.last_restock"),
        F.col("i.store_id"),
        F.col("p.price"),
        F.col("p.stock_qty"),
        F.col("s.city"),
        F.col("s.country"),
        F.last_day(F.col("p.launch_date")).alias("launch_date_month_end"),
    )
)
