from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .filter(F.col("quantity") == 80)
    .groupBy("store_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.max("last_restock").alias("last_last_restock"),
    )
)

result = (
    df_stores.alias("s")
    .join(inventory_by_store.alias("I"), F.col("s.id") == F.col("I.store_id"), "inner")
    .filter(F.col("I.quantity_count") > 4)
    .select(
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.col("I.quantity_count"),
        F.col("I.last_last_restock"),
    )
    .orderBy(F.col("opened_date").desc())
)
