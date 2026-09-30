from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNull() & F.col("discount").isNotNull())
    .select("id", "shipping_fee", "amount", F.length("channel").alias("channel_length"))
)
