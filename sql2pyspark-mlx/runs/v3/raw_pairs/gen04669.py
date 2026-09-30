from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") <= "2023-03-16")
    .groupBy("store_id", "product_id")
    .agg(F.min("revenue").alias("min_revenue"))
    .filter(F.col("min_revenue") >= 3500)
)
