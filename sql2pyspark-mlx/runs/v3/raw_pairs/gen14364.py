from pyspark.sql import functions as F

result = df_orders.select("shipping_fee", F.upper("channel").alias("channel_upper"))
