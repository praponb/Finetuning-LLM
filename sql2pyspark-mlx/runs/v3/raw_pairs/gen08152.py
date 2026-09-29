from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "right")
    .filter(F.col("ss.revenue") > 2200)
    .select(F.col("ss.id"), F.col("ss.units"), F.col("ss.store_id"), F.col("s.city"))
    .orderBy("store_id")
)
