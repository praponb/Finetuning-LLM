from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "outer")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .filter(F.col("ss.store_id").isin([2, 3, 4]))
    .groupBy(F.col("s.region"))
    .agg(F.max(F.col("ss.sale_date")).alias("latest_sale_date"))
    .orderBy("latest_sale_date")
)
