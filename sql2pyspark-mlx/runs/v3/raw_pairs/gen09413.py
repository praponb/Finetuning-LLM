from pyspark.sql import functions as F

result = df_orders.select("id", "customer_id", "channel")
