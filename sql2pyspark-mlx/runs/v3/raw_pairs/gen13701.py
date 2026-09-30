from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.opened_date") > "2023-12-06")
    .groupBy(F.col("s.region"), F.col("s.city"))
    .agg(F.count(F.col("ss.revenue")).alias("revenue_count"))
    .orderBy("city", "region")
    .limit(5)
)
