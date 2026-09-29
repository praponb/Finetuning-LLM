from pyspark.sql import functions as F

valid_products = (
    df_products
    .filter(((F.col("launch_date") > "2023-11-10") & (F.size("tags") > 1)) | (F.col("price") <= 970))
    .withColumn("stock_qty_whole", F.floor("stock_qty"))
)

agg = (
    valid_products
    .groupBy("category")
    .agg(F.avg("stock_qty_whole").alias("avg_stock_qty_whole"))
)

result = (
    agg
    .filter(F.col("avg_stock_qty_whole") >= 59)
    .select("category", "avg_stock_qty_whole")
)
