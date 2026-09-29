from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .filter(F.col("i.store_id").isin([1, 5]) & F.col("i.product_id").isin([4, 5]))
    .select(
        F.col("i.store_id"),
        F.col("i.product_id"),
        F.col("i.quantity"),
        F.col("s.opened_date"),
    )
    .orderBy("store_id")
)
