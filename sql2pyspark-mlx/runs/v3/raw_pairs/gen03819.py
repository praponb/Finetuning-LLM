from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 3, 4]))
    .groupBy("store_id")
    .agg(
        F.count("*").alias("num_inventory"),
        F.avg("quantity").alias("avg_quantity"),
    )
)

result = (
    df_stores.alias("s")
    .join(inventory_by_store.alias("ibs"), F.col("s.id") == F.col("ibs.store_id"), "inner")
    .filter(F.col("ibs.avg_quantity") > 240)
    .select(
        F.col("s.region"),
        F.col("s.store_name"),
        F.col("ibs.num_inventory"),
        F.col("ibs.avg_quantity"),
    )
    .orderBy(F.col("num_inventory").desc(), "avg_quantity")
)
