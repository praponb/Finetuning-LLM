from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(~F.col("s.region").isin(["Central", "West"]) & F.col("i.store_id").isin([1, 3, 4]))
    .select(F.col("i.store_id"), F.col("s.opened_date"), F.col("s.store_name"))
)
