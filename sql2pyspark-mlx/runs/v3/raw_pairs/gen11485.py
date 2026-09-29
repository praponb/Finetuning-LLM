from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("%Yog%"))
    .groupBy("category")
    .agg(
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"),
    )
)
