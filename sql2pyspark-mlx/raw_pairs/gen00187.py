from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "right")
    .filter(F.col("i.store_id").isin([1, 3, 4, 5]) | (F.col("i.quantity") >= 114))
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.product_id"),
        F.col("s.city"),
        F.date_sub(F.col("i.last_restock"), 42).alias("last_restock_minus_42d"),
    )
)
