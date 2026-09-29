from pyspark.sql import functions as F

store_stats = (
    df_inventory
    .filter(F.col("quantity") >= 141)
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.sum("quantity").alias("total_quantity"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("S"), F.col("s.id") == F.col("S.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.opened_date"),
        F.col("S.stddev_quantity"),
        F.col("S.total_quantity"),
    )
)
