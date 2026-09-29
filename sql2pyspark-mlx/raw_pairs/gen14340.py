from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .filter(F.col("store_id") == 4)
    .groupBy("store_id")
    .agg(
        F.min("last_restock").alias("min_last_restock"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.region"),
        F.col("s.store_name"),
        F.col("t.min_last_restock"),
        F.col("t.quantity_range"),
    )
)
