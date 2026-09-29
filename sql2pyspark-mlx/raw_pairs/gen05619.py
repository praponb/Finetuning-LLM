from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.city"))
    .agg(F.countDistinct(F.col("i.quantity")).alias("unique_stores"))
    .orderBy("unique_stores")
)
