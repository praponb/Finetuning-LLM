from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "right")
    .groupBy(F.quarter(F.col("s.opened_date")).alias("opened_quarter"))
    .agg(F.count("*").alias("store_sale_count"))
)
