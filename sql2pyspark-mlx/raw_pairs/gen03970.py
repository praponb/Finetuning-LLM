from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.max("stock_qty").alias("max_stock_qty"),
        F.round(F.avg("price"), 2).alias("avg_price"),
    )
    .filter(F.col("max_stock_qty") > 130)
    .select("category", "max_stock_qty", "avg_price")
)
