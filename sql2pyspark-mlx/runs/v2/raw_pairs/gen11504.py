from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.store_id").isin([3, 5, 6]))
    .groupBy(F.col("p.category"))
    .agg(F.max(F.col("ss.sale_date")).alias("latest_sale_date"))
)
