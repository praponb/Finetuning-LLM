from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("ss.store_id"),
        F.col("ss.units"),
        F.col("ss.revenue"),
        F.col("s.city"),
    )
)
