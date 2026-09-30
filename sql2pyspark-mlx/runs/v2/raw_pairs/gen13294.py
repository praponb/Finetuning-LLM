from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .filter(F.col("product_id").isin([4, 5, 8, 10]))
    .groupBy("store_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.countDistinct("quantity").alias("unique_products"),
    )
)

result = (
    df_stores.alias("s")
    .join(inventory_by_store.alias("ibs"), F.col("s.id") == F.col("ibs.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.store_name"),
        F.col("ibs.quantity_range"),
        F.col("ibs.unique_products"),
    )
)
