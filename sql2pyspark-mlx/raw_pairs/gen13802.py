from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .filter(F.col("t.quantity_count") > 9)
    .select(
        F.col("s.region"),
        F.col("s.store_name"),
        F.col("t.avg_quantity"),
        F.col("t.quantity_count"),
    )
)
