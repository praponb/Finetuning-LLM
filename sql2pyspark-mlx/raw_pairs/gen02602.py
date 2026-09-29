from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.brand"), F.year(F.col("p.launch_date")).alias("launch_year"))
    .agg(F.count(F.col("ss.revenue")).alias("revenue_count"))
    .filter(F.col("revenue_count") > 5)
    .orderBy("brand", "launch_year")
    .limit(10)
)
