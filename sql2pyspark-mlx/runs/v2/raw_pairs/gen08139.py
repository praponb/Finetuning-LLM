from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .filter(F.col("last_restock") < "2024-10-01")
    .groupBy("store_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)

result = (
    df_stores.alias("s")
    .join(inventory_by_store.alias("I"), F.col("s.id") == F.col("I.store_id"), "inner")
    .filter(F.col("I.quantity_range") > 74)
    .select(F.col("s.opened_date"), F.col("s.city"), F.col("I.quantity_range"))
    .orderBy(F.col("quantity_range").desc())
)
