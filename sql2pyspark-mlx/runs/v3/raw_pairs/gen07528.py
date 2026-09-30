from pyspark.sql import functions as F

result = df_orders.filter((F.col("customer_id") == 29) & (F.col("shipping_fee") <= 31))
