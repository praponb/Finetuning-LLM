from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("i.store_id").isin([3, 5]) & (F.col("p.stock_qty") == 317))
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.last_restock"),
        F.col("p.stock_qty"),
    )
)
