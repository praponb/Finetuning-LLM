from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .filter(F.col("store_id").isin([1, 4, 5, 6]))
    .groupBy("store_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.max("last_restock").alias("latest_last_restock"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .select(
        F.col("s.sqft"),
        F.col("s.store_name"),
        F.col("t.quantity_count"),
        F.col("t.latest_last_restock"),
    )
    .orderBy(F.col("quantity_count").desc())
)
