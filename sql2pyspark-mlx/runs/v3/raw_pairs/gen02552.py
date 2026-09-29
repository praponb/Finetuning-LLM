from pyspark.sql import functions as F

store_stats = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.max("last_restock").alias("last_last_restock"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("ss.stddev_quantity") >= 37)
    .select(
        F.col("s.opened_date"),
        F.col("s.city"),
        F.col("ss.last_last_restock"),
        F.col("ss.stddev_quantity"),
    )
    .orderBy("city")
)
