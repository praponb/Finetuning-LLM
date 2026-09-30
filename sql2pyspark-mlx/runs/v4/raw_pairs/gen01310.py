from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.max("stock_qty").alias("max_stock_qty"),
    )
)
