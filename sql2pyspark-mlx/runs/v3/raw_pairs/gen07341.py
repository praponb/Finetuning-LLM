from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").between(720, 1140))
    .groupBy("brand")
    .agg(
        F.min("stock_qty").alias("min_stock_qty"),
        F.max("cost").alias("max_cost"),
    )
)
