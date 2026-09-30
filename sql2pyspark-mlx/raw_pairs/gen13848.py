from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("ss.revenue") >= 3300)
    .select(F.col("ss.product_id"), F.col("ss.units"), F.col("s.sqft"), F.col("s.city"))
)
