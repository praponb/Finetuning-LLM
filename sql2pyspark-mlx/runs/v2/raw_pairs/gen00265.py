from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.count("*").alias("inventory_count"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("s.region"),
        F.col("s.sqft"),
        F.col("ss.inventory_count"),
        F.col("ss.quantity_range"),
    )
    .orderBy(F.col("quantity_range").desc())
)
