from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(F.col("ss.id"), F.col("ss.store_id"), F.col("ss.revenue"), F.col("s.city"))
    .orderBy(F.col("store_id").desc(), F.col("id").desc())
    .limit(10)
)
