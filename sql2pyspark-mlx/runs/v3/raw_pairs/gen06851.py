from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("ss.revenue"),
        F.col("ss.store_id"),
        F.col("s.sqft"),
        F.col("s.store_name"),
    )
)
