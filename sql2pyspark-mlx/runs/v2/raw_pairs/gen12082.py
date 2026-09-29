from pyspark.sql import functions as F

store_stats = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.sum("quantity").alias("total_quantity"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("s.opened_date"),
        F.col("s.city"),
        F.col("ss.min_quantity"),
        F.col("ss.total_quantity"),
    )
    .orderBy("city", "opened_date")
)
