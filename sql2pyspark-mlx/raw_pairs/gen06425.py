from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "left")
    .groupBy(F.col("s.region"))
    .agg(F.countDistinct(F.col("ss.revenue")).alias("unique_stores"))
)
