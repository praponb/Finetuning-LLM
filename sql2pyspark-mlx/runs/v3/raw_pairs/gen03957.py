from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.avg("quantity").alias("avg_quantity"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("ss.avg_quantity") > 160)
    .select(
        F.col("s.id"),
        F.col("s.store_name"),
        F.col("s.region"),
        F.col("ss.quantity_count"),
        F.col("ss.avg_quantity"),
    )
)
