from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "right")
    .select(F.col("ss.store_id"), F.col("ss.product_id"), F.col("s.city"))
    .orderBy(F.col("product_id").desc())
)
