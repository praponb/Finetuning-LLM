from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id", "product_id")
    .agg(F.sum("revenue").alias("total_revenue"))
)
