from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.month("launch_date").alias("launch_month"), "category")
    .agg(
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
        F.avg("price").alias("avg_price"),
    )
    .orderBy(F.col("avg_price").desc())
)
