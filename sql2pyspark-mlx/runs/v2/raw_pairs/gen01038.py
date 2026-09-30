from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").between(0.11, 0.4))
    .groupBy("status", "channel")
    .agg(F.countDistinct("shipping_fee").alias("unique_channels"))
)
