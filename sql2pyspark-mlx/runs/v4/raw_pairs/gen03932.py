from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(F.max("order_date").alias("last_order_date"))
    .orderBy("channel", "last_order_date")
)
