from pyspark.sql import functions as F

store_stats = (
    df_inventory
    .groupBy("store_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("S"), F.col("s.id") == F.col("S.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.store_name"),
        F.col("s.city"),
        F.col("S.avg_quantity"),
    )
    .orderBy("avg_quantity")
)
