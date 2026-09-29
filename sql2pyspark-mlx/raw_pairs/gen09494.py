from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.date_format("launch_date", "yyyy-MM").alias("launch_month"))
    .agg(F.count("stock_qty").alias("stock_qty_count"))
    .orderBy("stock_qty_count")
)
