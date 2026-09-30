from pyspark.sql import functions as F

result = (
    df_products
    .agg(
        F.count("cost").alias("cost_count"),
        F.min("stock_qty").alias("min_stock_qty"),
        F.round(F.stddev("price"), 2).alias("stddev_price"),
    )
)
