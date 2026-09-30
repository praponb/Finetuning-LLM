from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.filter(F.col("opened_date") >= "2024-02-01").alias("s"), F.col("ss.store_id") == F.col("s.id"), "left_anti")
    .select(F.col("ss.id"), F.col("ss.revenue"), F.col("ss.units"))
    .orderBy(F.col("id").desc(), "revenue")
)
