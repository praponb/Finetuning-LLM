from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(F.count("shipping_fee").alias("shipping_fee_count"))
)
