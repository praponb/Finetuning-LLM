from pyspark.sql import functions as F

result = (
    df_orders
    .select("id", "order_date", "channel", F.upper("channel").alias("channel_upper"))
)
