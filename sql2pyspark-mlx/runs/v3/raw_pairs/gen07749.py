from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("stock_qty") >= 434, "high").when(F.col("stock_qty") >= 54, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(
        F.max("launch_date").alias("last_launch_date"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
    .filter(F.col("stddev_cost") >= 22)
    .select("stock_qty_bucket", "last_launch_date", "stddev_cost")
    .orderBy("stock_qty_bucket")
    .limit(10)
)
