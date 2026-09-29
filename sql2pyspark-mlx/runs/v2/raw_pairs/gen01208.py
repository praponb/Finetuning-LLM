from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("s.city"),
        F.col("s.sqft"),
        F.col("ss.total_quantity"),
        F.col("ss.avg_quantity"),
    )
    .orderBy(F.col("total_quantity").desc())
)
