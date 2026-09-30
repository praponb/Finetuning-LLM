from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .groupBy(F.col("s.region"))
    .agg(F.max(F.col("ss.sale_date")).alias("latest_sale_date"))
    .orderBy("region", "latest_sale_date")
    .limit(5)
)
