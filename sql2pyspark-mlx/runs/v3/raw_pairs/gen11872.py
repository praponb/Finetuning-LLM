from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.city") == "London")
    .select(F.col("ss.product_id"), F.col("s.store_name"))
)
