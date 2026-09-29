from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("s.id") == F.col("i.store_id"), "inner")
    .filter(F.col("s.city") == "Tokyo")
    .groupBy(F.col("s.id"), F.col("s.store_name"))
    .agg(
        F.count(F.col("i.id")).alias("inventory_count"),
        (F.max(F.col("i.quantity")) - F.min(F.col("i.quantity"))).alias("quantity_range"),
        F.countDistinct(F.col("i.quantity")).alias("unique_stores"),
    )
    .orderBy("store_name")
)
