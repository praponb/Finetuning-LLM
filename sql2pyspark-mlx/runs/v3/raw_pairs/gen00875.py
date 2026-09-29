from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(
        F.max("order_date").alias("latest_order_date"),
        F.max("amount").alias("max_amount"),
        F.min("order_date").alias("first_order_date"),
    )
)
