from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
)

result = (
    df_stores.alias("stores")
    .join(store_totals.alias("StoreTotals"), F.col("stores.id") == F.col("StoreTotals.store_id"), "inner")
    .select(
        F.col("stores.sqft"),
        F.col("StoreTotals.quantity_count"),
        F.col("StoreTotals.avg_quantity"),
    )
    .orderBy("avg_quantity", F.col("quantity_count").desc())
)
