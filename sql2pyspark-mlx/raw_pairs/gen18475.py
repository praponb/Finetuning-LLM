from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") < "2024-05-01")
    .groupBy("channel", F.date_trunc("MONTH", "order_date").alias("order_month"))
    .agg(
        F.max("order_date").alias("latest_order_date"),
        F.sum(F.when(F.col("channel") == "store", 1).otherwise(0)).alias("store_count"),
    )
    .filter(F.col("store_count") > 60)
)
