from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "outer")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "left")
    .filter(F.col("i.store_id").isin([2, 5]) & F.col("p.supplier_id").isNull())
    .select(
        F.col("i.quantity"),
        F.col("i.last_restock"),
        F.col("s.city"),
        F.col("p.stock_qty"),
        F.col("p.supplier_id"),
    )
)
