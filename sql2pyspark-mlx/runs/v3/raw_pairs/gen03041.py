from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .filter(F.col("revenue").isNull())
    .groupBy("store_id")
    .agg(F.max("sale_date").alias("latest_sale_date"))
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(F.col("s.region"), F.col("s.sqft"), F.col("ss.latest_sale_date"))
    .orderBy("latest_sale_date")
)
