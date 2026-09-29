from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("s.store_name").like("%Riversi%"))
    .select(
        F.col("i.store_id"),
        F.col("i.quantity"),
        F.col("s.sqft"),
        F.col("p.stock_qty"),
        F.col("p.cost"),
    )
)
