from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.store_name").like("%Dow%"))
    .select(
        F.col("i.store_id"),
        F.col("i.product_id"),
        F.col("p.stock_qty"),
        F.col("p.launch_date"),
        F.col("s.store_name"),
        F.date_format(F.col("s.opened_date"), "MMM yyyy").alias("opened_date_label"),
    )
    .orderBy("store_name")
)
