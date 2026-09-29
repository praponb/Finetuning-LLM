from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(F.sum("shipping_fee").alias("total_shipping_fee"))
    .filter(F.col("total_shipping_fee") < 170)
    .select("channel", "total_shipping_fee")
    .orderBy("total_shipping_fee", F.col("channel").desc())
)
