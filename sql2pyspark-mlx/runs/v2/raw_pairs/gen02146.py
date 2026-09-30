from pyspark.sql import functions as F

result = df_orders.select("discount", "order_date")
