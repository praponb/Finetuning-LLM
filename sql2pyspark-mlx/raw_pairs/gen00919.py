from pyspark.sql import functions as F

result = df_orders.select("id", "channel", F.trim("status").alias("clean_status"))
