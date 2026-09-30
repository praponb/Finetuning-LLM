from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id") == 23)
    .groupBy("brand")
    .agg(
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
        F.min("stock_qty").alias("min_stock_qty"),
    )
    .filter(F.col("avg_stock_qty") < 60)
)
