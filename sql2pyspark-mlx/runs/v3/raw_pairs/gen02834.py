from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").between(121, 373))
    .withColumn("stock_qty_num", F.col("stock_qty").cast("string"))
)
