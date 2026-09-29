from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.min("last_restock").alias("first_last_restock"),
        F.min("last_restock").alias("min_last_restock"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .select(F.col("s.city"), F.col("t.first_last_restock"), F.col("t.min_last_restock"))
)
