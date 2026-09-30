from pyspark.sql import functions as F

result = (
    df_store_sales
    .agg(F.count("*").alias("store_sale_count"))
    .orderBy(F.col("store_sale_count").desc())
)
