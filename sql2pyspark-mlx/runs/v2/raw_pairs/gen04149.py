from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.filter(F.col("region") != "North").alias("s"), F.col("ss.store_id") == F.col("s.id"), "left_anti")
    .filter(F.col("ss.product_id").isin([2, 7, 9, 10]))
    .select(F.col("ss.id"), F.col("ss.product_id"))
)
