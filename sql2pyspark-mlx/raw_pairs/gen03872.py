from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("amount") <= 1580) | (F.col("store_id") == 39))
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(
        F.max("order_date").alias("latest_order_date"),
        F.min("order_date").alias("first_order_date"),
    )
    .orderBy("latest_order_date")
)
