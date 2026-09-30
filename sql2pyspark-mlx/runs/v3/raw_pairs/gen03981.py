from pyspark.sql import functions as F

result = (
    df_orders
    .select("id", "shipping_fee", F.substring("channel", 1, 2).alias("channel_prefix"))
)
