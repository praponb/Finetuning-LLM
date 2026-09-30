from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.city") == "Chiang Mai")
    .groupBy(F.col("s.region"))
    .agg(
        F.max(F.col("ss.revenue")).alias("max_revenue"),
        F.count(F.col("ss.units")).alias("units_count"),
    )
)
