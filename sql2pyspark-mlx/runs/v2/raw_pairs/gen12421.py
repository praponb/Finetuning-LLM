from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.quarter("launch_date").alias("launch_quarter"))
    .agg(F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"))
)
