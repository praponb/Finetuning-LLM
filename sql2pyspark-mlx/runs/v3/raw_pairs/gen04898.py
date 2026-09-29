from pyspark.sql import functions as F

filtered_orders = df_orders.filter(F.col("status") == "COMPLETED")

orders_stats = (
    filtered_orders
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(
        F.min("discount").alias("min_discount"),
        F.count("amount").alias("amount_count"),
    )
)

result = orders_stats.orderBy("min_discount")
