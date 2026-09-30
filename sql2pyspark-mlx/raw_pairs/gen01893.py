from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("ss.store_id") == F.col("s.id"), "left")
    .select(
        F.col("s.sqft"),
        F.col("ss.sale_date"),
        F.col("ss.store_id"),
        F.col("ss.revenue"),
        F.round(F.col("ss.units"), 1).alias("units_rounded"),
    )
)
