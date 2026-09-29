from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel", "status")
    .agg(F.count("shipping_fee").alias("shipping_fee_count"))
    .orderBy("status")
)
