from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.sqft") == 9793)
    .groupBy(F.year(F.col("s.opened_date")).alias("opened_year"))
    .agg(F.count("*").alias("store_sale_count"))
    .orderBy("store_sale_count", F.col("opened_year").desc())
    .limit(10)
)
