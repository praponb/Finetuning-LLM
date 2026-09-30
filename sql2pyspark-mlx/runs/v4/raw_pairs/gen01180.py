from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost").between(200, 630) & F.col("product_name").like("%Hub%"))
    .groupBy(F.month("launch_date").alias("launch_month"))
    .agg(
        F.round(F.stddev("price"), 2).alias("stddev_price"),
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
    )
)
