from pyspark.sql import functions as F

result = df_orders.select("channel", "customer_id", "shipping_fee")
