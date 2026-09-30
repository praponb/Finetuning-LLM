from pyspark.sql import functions as F

result = df_orders.select("id", F.date_sub("order_date", 7).alias("week_before"))
