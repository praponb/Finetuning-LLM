from pyspark.sql import functions as F

store_stats = (
    df_inventory
    .filter(F.col("store_id") == 5)
    .groupBy("store_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .filter(F.col("t.quantity_count") > 10)
    .select(
        F.col("s.id"),
        F.col("s.opened_date"),
        F.col("t.min_quantity"),
        F.col("t.quantity_count"),
    )
)
