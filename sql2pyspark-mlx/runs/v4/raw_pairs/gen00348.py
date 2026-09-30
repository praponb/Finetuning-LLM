from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
    )
)
