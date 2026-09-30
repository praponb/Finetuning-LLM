from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("i.last_restock") > "2024-02-29")
    .groupBy(F.col("s.id"), F.col("s.city"))
    .agg(
        F.count(F.col("i.id")).alias("inventory_count"),
        F.sum(F.col("i.quantity")).alias("total_quantity"),
    )
    .orderBy("inventory_count")
)
