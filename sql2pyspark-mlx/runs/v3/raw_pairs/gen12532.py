from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("price") >= 1170) & F.col("supplier_id").isNull())
    .cube("category", F.month("launch_date").alias("launch_month"))
    .agg(F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"))
)
