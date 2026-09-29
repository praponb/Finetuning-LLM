from pyspark.sql import functions as F

store_stats = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.countDistinct("quantity").alias("unique_stores"),
        F.count("quantity").alias("quantity_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.col("ss.quantity_count") >= 9)
    .select(
        F.col("s.id"),
        F.col("s.sqft"),
        F.col("s.opened_date"),
        F.col("ss.unique_stores"),
        F.col("ss.quantity_count"),
    )
)
