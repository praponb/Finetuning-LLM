from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .filter(F.col("store_id") == 4)
    .groupBy("store_id")
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("ss.avg_quantity") > 51)
    .select(
        F.col("s.id"),
        F.col("s.sqft"),
        F.col("s.opened_date"),
        F.col("ss.avg_quantity"),
    )
)
