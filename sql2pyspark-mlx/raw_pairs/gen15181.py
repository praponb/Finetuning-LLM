from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .groupBy(F.col("s.region"))
    .agg(F.count("*").alias("num_store_sales"))
    .filter(F.col("num_store_sales") > 1)
    .orderBy("num_store_sales", "region")
)
