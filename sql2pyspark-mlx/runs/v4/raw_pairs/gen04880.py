from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .filter(F.col("p.stock_qty").isNull())
    .select(
        F.col("i.quantity"),
        F.col("s.region"),
        F.col("p.stock_qty"),
        F.last_day(F.col("i.last_restock")).alias("last_restock_month_end"),
    )
    .orderBy(F.col("region").desc(), F.col("quantity").desc())
)
