from pyspark.sql import functions as F

store_summary = (
    df_store_sales
    .filter(F.col("revenue") >= 1500)
    .groupBy("store_id")
    .agg(
        F.count("units").alias("units_count"),
        F.sum("units").alias("total_units"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(
        F.col("s.city"),
        F.col("s.sqft"),
        F.col("ss.units_count"),
        F.col("ss.total_units"),
    )
)
