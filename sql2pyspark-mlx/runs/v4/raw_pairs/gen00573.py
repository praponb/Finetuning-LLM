from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "left")
    .filter((F.col("p.category") != "Grocery") & (F.col("i.quantity") > 139))
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.product_id"),
        F.col("p.stock_qty"),
        F.col("s.city"),
    )
    .orderBy(F.col("stock_qty").desc(), "city")
)
