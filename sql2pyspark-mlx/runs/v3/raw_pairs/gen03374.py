from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .filter(F.col("product_id").isin([2, 6, 10]))
    .groupBy("store_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.count("*").alias("inventory_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(inventory_by_store.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .select(
        F.col("s.city"),
        F.col("s.sqft"),
        F.col("t.avg_quantity"),
        F.col("t.inventory_count"),
    )
)
