from pyspark.sql import functions as F

store_stats = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.col("ss.quantity_range") >= 25)
    .select(
        F.col("s.sqft"),
        F.col("s.region"),
        F.col("ss.total_quantity"),
        F.col("ss.quantity_range"),
    )
)
