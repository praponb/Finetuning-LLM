from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("i.store_id").isin([1, 3, 6]))
    .groupBy(F.col("s.city"))
    .agg(
        F.min(F.col("i.quantity")).alias("min_quantity"),
        F.max(F.col("i.last_restock")).alias("latest_last_restock"),
    )
)
