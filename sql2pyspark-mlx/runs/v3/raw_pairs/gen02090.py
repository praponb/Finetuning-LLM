from pyspark.sql import functions as F

store_stats = (
    df_inventory
    .filter(F.month("last_restock").isin([1, 9, 10]))
    .groupBy("store_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.count("*").alias("row_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("ss.max_quantity"),
        F.col("ss.row_count"),
    )
)
