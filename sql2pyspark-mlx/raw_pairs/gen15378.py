from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .groupBy(F.month(F.col("s.opened_date")).alias("opened_month"), F.col("s.region"))
    .agg(F.countDistinct(F.col("i.quantity")).alias("unique_stores"))
)
