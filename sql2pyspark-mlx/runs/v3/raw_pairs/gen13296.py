from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.units") > 22)
    .groupBy(F.quarter(F.col("p.launch_date")).alias("launch_quarter"))
    .agg(F.sum(F.col("ss.revenue")).alias("total_revenue"))
    .filter(F.col("total_revenue") >= 14000)
    .orderBy(F.col("total_revenue").desc())
)
