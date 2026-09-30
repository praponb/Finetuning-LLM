from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") != 31)
    .groupBy("product_id")
    .agg(F.avg("revenue").alias("avg_revenue"))
)
