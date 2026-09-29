from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("s.id") == F.col("i.store_id"), "inner")
    .filter(F.year(F.col("i.last_restock")) == 2025)
    .groupBy(F.col("s.id"), F.col("s.region"))
    .agg(
        F.count(F.col("i.id")).alias("inventory_count"),
        F.max(F.col("i.quantity")).alias("max_quantity"),
        F.countDistinct(F.col("i.quantity")).alias("unique_stores"),
    )
    .orderBy(F.col("region").desc(), "id")
    .limit(20)
)
