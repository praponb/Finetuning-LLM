from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"),
        F.avg("stock_qty").alias("avg_stock_qty"),
    )
)
