from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .groupBy(F.col("s.city"))
    .agg(F.count("*").alias("row_count"))
    .filter(F.col("row_count") > 1)
    .orderBy(F.col("row_count").desc())
)
