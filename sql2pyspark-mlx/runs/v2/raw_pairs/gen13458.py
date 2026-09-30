from pyspark.sql import functions as F

filtered_orders = df_orders.filter(F.col("store_id").isin([34, 39]) & (F.col("status") == "SHIPPED"))

channel_summary = (
    filtered_orders
    .groupBy("channel")
    .agg(F.max("discount").alias("max_discount"))
)

result = channel_summary
