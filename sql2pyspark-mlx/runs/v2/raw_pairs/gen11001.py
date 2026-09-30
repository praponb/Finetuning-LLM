from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("amount") <= 1380) | ~F.col("status").isin(["SHIPPED", "RETURNED", "CANCELLED"]))
    .groupBy("store_id")
    .agg(F.max("order_date").alias("latest_order_date"))
)
