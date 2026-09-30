from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.count("*").alias("num_inventory"),
        F.countDistinct("quantity").alias("unique_products"),
    )
)

result = (
    df_stores.alias("s")
    .join(inventory_by_store.alias("ibs"), F.col("s.id") == F.col("ibs.store_id"), "left")
    .select(
        F.col("s.store_name"),
        F.col("s.city"),
        F.col("ibs.num_inventory"),
        F.col("ibs.unique_products"),
    )
    .orderBy(F.col("store_name").desc())
)
