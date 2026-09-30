from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("i.store_id") == F.col("s.id"), "inner")
    .select(F.col("s.region"), F.col("s.opened_date"), F.col("i.last_restock"))
)
