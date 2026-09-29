from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .groupBy("store_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)

result = (
    df_stores.alias("s")
    .join(inventory_by_store.alias("I"), F.col("s.id") == F.col("I.store_id"), "inner")
    .select(F.col("s.sqft"), F.col("s.store_name"), F.col("I.quantity_range"))
    .orderBy(F.col("sqft").desc(), F.col("store_name").desc())
)
