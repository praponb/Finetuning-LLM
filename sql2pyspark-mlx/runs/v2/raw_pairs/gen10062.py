from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.max("last_restock").alias("last_last_restock"),
    )
)

result = (
    df_stores.alias("s")
    .join(inventory_by_store.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .select(
        F.col("s.region"),
        F.col("s.sqft"),
        F.col("t.stddev_quantity"),
        F.col("t.last_last_restock"),
    )
)
