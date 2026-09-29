from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .filter(F.col("store_id").isin([1, 6]))
    .groupBy("store_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.count("quantity").alias("quantity_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("S"), F.col("s.id") == F.col("S.store_id"), "inner")
    .filter(F.col("S.quantity_count") >= 4)
    .select(
        F.col("s.city"),
        F.col("s.region"),
        F.col("S.quantity_range"),
        F.col("S.quantity_count"),
    )
)
