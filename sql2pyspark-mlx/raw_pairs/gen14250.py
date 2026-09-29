from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("s.id") == F.col("i.store_id"), "left")
    .filter(F.month(F.col("s.opened_date")).isin([2, 6, 9]))
    .groupBy(F.col("s.region"))
    .agg(F.count(F.col("i.id")).alias("inventory_count"))
)
