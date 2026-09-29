from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .pivot("category", ["Sports", "Home"])
    .agg(F.avg("stock_qty").alias("amount"), F.count(F.lit(1)).alias("n"))
)
