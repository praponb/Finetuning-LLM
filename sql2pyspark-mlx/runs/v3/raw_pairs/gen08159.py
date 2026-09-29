from pyspark.sql import functions as F

result = df_orders.select("discount", F.round("discount", 2).alias("discount_rounded"))
