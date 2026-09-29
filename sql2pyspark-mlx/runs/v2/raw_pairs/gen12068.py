from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.opened_date") <= "2024-02-01")
    .groupBy(F.col("s.id"), F.col("s.store_name"))
    .agg(
        F.count(F.col("i.id")).alias("inventory_count"),
        F.countDistinct(F.col("i.quantity")).alias("unique_products"),
    )
    .orderBy("inventory_count", "id", "store_name")
    .limit(10)
)
