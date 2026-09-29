from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") > 410)
    .groupBy("category")
    .agg(F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"))
    .filter(F.col("stddev_stock_qty") > 61)
    .orderBy(F.col("stddev_stock_qty").desc(), F.col("category").desc())
)
