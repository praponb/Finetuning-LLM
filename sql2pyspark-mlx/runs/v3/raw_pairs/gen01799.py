from pyspark.sql import functions as F

store_sales_by_store = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.max("units").alias("max_units"))
)

result = (
    df_stores.alias("s")
    .join(store_sales_by_store.alias("ssbs"), F.col("s.id") == F.col("ssbs.store_id"), "left")
    .select(F.col("s.id"), F.col("s.city"), F.col("s.region"), F.col("ssbs.max_units"))
    .orderBy("region")
)
