from pyspark.sql import functions as F

store_stats = (
    df_inventory
    .filter(F.col("quantity") < 169)
    .groupBy("store_id")
    .agg(F.min("last_restock").alias("first_last_restock"))
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.opened_date"),
        F.col("s.sqft"),
        F.col("ss.first_last_restock"),
    )
)
