from pyspark.sql import functions as F

result = df_orders.select("amount", "status", F.floor("amount").alias("amount_whole"))
