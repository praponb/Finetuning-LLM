from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(
        F.min("shipping_fee").alias("min_shipping_fee"),
        F.max("order_date").alias("latest_order_date"),
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_count"),
    )
    .filter(F.col("web_count") > 3)
    .drop("web_count")
)
