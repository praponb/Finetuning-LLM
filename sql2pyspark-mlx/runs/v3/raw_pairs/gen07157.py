from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand", F.year("launch_date").alias("launch_year"))
    .agg((F.max("stock_qty") - F.min("stock_qty")).alias("stock_qty_range"))
    .orderBy("brand")
)
