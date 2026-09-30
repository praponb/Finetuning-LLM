from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg((F.max("stock_qty") - F.min("stock_qty")).alias("stock_qty_range"))
)
