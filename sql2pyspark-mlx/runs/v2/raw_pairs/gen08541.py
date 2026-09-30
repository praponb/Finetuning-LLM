from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "outer")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .filter(F.col("ss.units") <= 27)
    .groupBy(F.col("s.city"))
    .agg(F.max(F.col("ss.revenue")).alias("max_revenue"))
    .orderBy("max_revenue")
)
