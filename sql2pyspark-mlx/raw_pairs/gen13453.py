from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "outer")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "right")
    .groupBy(F.col("p.brand"))
    .agg(F.countDistinct(F.col("ss.revenue")).alias("unique_products"))
    .filter(F.col("unique_products") >= 3)
)
