from pyspark.sql import functions as F

result = df_orders.select("discount", "customer_id", "order_date")
