from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
        F.min("order_date").alias("min_order_date"),
    )
)
