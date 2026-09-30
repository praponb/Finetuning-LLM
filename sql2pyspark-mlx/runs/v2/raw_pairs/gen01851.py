from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("i.quantity") > 143)
    .groupBy(F.col("s.city"))
    .agg(
        F.max(F.col("i.last_restock")).alias("last_last_restock"),
        F.min(F.col("i.quantity")).alias("min_quantity"),
    )
    .orderBy("last_last_restock", "city")
)
