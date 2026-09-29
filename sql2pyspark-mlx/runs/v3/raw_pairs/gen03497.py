from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status", F.quarter("order_date").alias("order_quarter"))
    .agg(
        F.max("order_date").alias("latest_order_date"),
        F.min("amount").alias("min_amount"),
    )
)
