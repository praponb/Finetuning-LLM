from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .filter(F.year("last_restock") == 2025)
    .groupBy("store_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .select(
        F.col("s.store_name"),
        F.col("s.sqft"),
        F.col("t.total_quantity"),
        F.col("t.quantity_range"),
    )
)
