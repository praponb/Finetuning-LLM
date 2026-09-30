from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.sqft") < 8453)
    .select(F.col("ss.id"), F.col("ss.product_id"), F.col("s.city"), F.col("s.region"))
)
