from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.min("last_restock").alias("first_last_restock"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("ss.quantity_count") > 8)
    .select(
        F.col("s.id"),
        F.col("s.region"),
        F.col("s.sqft"),
        F.col("ss.quantity_count"),
        F.col("ss.first_last_restock"),
    )
)
