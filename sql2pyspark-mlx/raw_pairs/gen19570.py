from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.col("ss.store_id").isin([3, 4, 5]))
    .select(F.col("ss.id"), F.col("ss.units"), F.col("ss.store_id"), F.col("s.region"))
)
