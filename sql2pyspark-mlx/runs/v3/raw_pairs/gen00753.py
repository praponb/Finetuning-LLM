from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("s.city") != "Chiang Mai")
    .select(
        F.col("s.city"),
        F.col("s.region"),
        F.col("ss.revenue"),
        F.col("ss.store_id"),
        F.col("ss.sale_date"),
        F.round(F.col("ss.units"), 1).alias("units_rounded"),
    )
)
