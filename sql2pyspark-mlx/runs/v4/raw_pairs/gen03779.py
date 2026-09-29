from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .groupBy("store_id")
    .agg(F.max("quantity").alias("max_quantity"))
)

result = (
    df_stores.alias("s")
    .join(inventory_by_store.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .select(F.col("s.id"), F.col("s.opened_date"), F.col("t.max_quantity"))
)
