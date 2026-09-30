from pyspark.sql import functions as F

result = (
    df_orders
    .withColumn("channel_title", F.initcap(F.trim("channel")))
    .withColumn("shipping_fee_int", F.col("shipping_fee").cast("int"))
)
