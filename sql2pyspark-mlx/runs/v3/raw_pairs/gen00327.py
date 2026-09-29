from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.filter(F.col("opened_date") >= "2025-01-01").alias("s"), F.col("ss.store_id") == F.col("s.id"), "left_anti")
    .select(F.col("ss.id"), F.col("ss.product_id"))
    .orderBy(F.col("product_id").desc(), F.col("id").desc())
    .limit(3)
)
