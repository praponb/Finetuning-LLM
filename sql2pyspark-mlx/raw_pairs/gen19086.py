from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("p.category"))
    .agg(F.avg(F.col("ss.revenue")).alias("avg_revenue"))
    .filter(F.col("avg_revenue") > 3100)
)
