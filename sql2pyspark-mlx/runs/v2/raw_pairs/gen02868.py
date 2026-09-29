from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .filter(F.col("s.city") == "Singapore")
    .select(
        F.col("i.last_restock"),
        F.col("i.quantity"),
        F.col("p.stock_qty"),
        F.col("s.opened_date"),
    )
    .orderBy(F.col("stock_qty").desc(), "opened_date")
)
