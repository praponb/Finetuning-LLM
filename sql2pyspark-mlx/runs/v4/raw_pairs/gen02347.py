from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .groupBy(F.month(F.col("p.launch_date")).alias("launch_month"))
    .agg(F.avg(F.col("ss.revenue")).alias("avg_revenue"))
    .filter(F.col("avg_revenue") > 3200)
)
