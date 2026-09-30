from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("ss.store_id").isin([2, 3, 6]))
    .groupBy(F.col("p.supplier_id"))
    .agg(F.count("*").alias("store_sale_count"))
    .filter(F.col("store_sale_count") > 1)
    .orderBy(F.col("store_sale_count").desc())
)
