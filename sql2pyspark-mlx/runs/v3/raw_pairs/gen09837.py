from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 5, 6]))
    .groupBy("store_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("s.sqft"),
        F.col("s.region"),
        F.col("ss.quantity_range"),
        F.col("ss.stddev_quantity"),
    )
    .orderBy(F.col("quantity_range").desc())
)
