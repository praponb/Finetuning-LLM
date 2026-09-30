from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("i.store_id").isin([2, 4, 5]))
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.quantity"),
        F.col("s.region"),
        F.col("p.stock_qty"),
        F.col("p.supplier_id"),
    )
    .orderBy(F.col("supplier_id").desc())
)
