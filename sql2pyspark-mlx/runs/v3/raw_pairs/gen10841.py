from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.filter(F.col("region").isin(["Central", "West", "South"])).alias("s"), F.col("ss.store_id") == F.col("s.id"), "left_semi")
    .filter(F.col("ss.product_id").isin([5, 9]))
    .select(F.col("ss.id"), F.col("ss.store_id"))
    .orderBy(F.col("store_id").desc(), F.col("id").desc())
    .limit(10)
)
