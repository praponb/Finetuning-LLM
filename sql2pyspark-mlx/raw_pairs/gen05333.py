from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("s.id") == F.col("i.store_id"), "left")
    .filter(F.col("i.product_id").isin([5, 6]) & (F.col("i.quantity") != 240))
    .select(
        F.col("s.store_name"),
        F.col("i.quantity"),
        F.col("i.product_id"),
        F.weekofyear(F.col("i.last_restock")).alias("last_restock_weekofyear"),
    )
)
