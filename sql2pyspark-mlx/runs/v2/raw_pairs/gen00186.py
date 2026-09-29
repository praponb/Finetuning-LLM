from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("ss.product_id").isin([5, 7, 10]))
    .groupBy(F.col("p.category"))
    .agg(F.countDistinct(F.col("ss.revenue")).alias("unique_stores"))
)
