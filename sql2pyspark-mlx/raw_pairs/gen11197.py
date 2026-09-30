from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "outer")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .filter((F.col("i.product_id") == 6) & ~F.col("s.city").isin(["Tokyo", "London"]))
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("i.quantity"),
        F.col("p.stock_qty"),
        F.col("s.store_name"),
        F.col("s.city"),
    )
)
