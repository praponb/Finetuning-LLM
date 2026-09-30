from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
)

result = (
    df_stores.alias("stores")
    .join(store_totals.alias("t"), F.col("stores.id") == F.col("t.store_id"), "left")
    .select(
        F.col("stores.id"),
        F.col("stores.sqft"),
        F.col("t.max_quantity"),
        F.col("t.avg_quantity"),
    )
)
