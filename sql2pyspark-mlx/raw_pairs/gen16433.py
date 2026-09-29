from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        (F.max("units") - F.min("units")).alias("units_range"),
        F.sum("units").alias("total_units"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.col("ss.units_range") >= 50)
    .select(
        F.col("s.opened_date"),
        F.col("s.sqft"),
        F.col("ss.units_range"),
        F.col("ss.total_units"),
    )
)
