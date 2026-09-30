from pyspark.sql import functions as F

result = df_orders.select("id", "store_id", "customer_id")
