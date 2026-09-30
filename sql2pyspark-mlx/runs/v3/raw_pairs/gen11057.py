from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id") == 2)
    .agg(F.count("*").alias("row_count"))
)
