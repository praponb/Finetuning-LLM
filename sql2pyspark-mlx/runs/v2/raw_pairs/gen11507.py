from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.store_id").isin([1, 2, 4, 6]))
    .select(
        F.col("s.store_name"),
        F.col("s.city"),
        F.col("s.sqft"),
        F.col("ss.store_id"),
    )
)
