from pyspark.sql import functions as F

store_stats = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.max("last_restock").alias("last_last_restock"),
        F.avg("quantity").alias("avg_quantity"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.store_name"),
        F.col("s.region"),
        F.col("t.last_last_restock"),
        F.col("t.avg_quantity"),
    )
    .orderBy("id")
)
