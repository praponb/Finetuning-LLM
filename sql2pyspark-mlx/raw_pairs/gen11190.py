from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("s.sqft") > 5377)
    .select(
        F.col("s.city"),
        F.col("s.store_name"),
        F.col("s.sqft"),
        F.col("ss.product_id"),
        F.col("ss.units"),
        F.col("ss.store_id"),
    )
)
