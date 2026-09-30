from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([3, 4, 6]))
    .agg(F.min("revenue").alias("min_revenue"))
)
