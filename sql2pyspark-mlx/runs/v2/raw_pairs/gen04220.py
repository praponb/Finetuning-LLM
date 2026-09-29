from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.avg("stock_qty").alias("avg_stock_qty"),
        F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_count"),
    )
    .filter(F.col("avg_stock_qty") > 170)
)
