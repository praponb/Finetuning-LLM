from pyspark.sql import functions as F

store_stats = (
    df_inventory
    .filter(F.month("last_restock").isin([1, 2, 12]))
    .groupBy("store_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.countDistinct("quantity").alias("unique_products"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.col("ss.unique_products") >= 4)
    .select(
        F.col("s.region"),
        F.col("s.opened_date"),
        F.col("ss.min_quantity"),
        F.col("ss.unique_products"),
    )
    .orderBy("opened_date")
)
