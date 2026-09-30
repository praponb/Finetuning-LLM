from pyspark.sql import functions as F

result = (
    df_shipments
    .select(F.col("delivered_date").alias("event_date"))
    .unionByName(df_orders.select(F.col("order_date").alias("event_date")))
)
