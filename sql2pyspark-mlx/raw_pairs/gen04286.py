from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category", F.date_format("launch_date", "yyyy-MM").alias("launch_month"))
    .agg(F.count("stock_qty").alias("stock_qty_count"))
    .filter(F.col("stock_qty_count") >= 9)
    .orderBy("launch_month")
)
