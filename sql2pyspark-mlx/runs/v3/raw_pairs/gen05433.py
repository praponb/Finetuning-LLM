from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel", "customer_id")
    .agg(F.max("order_date").alias("latest_order_date"))
    .orderBy(F.col("latest_order_date").desc(), F.col("customer_id").desc())
)
