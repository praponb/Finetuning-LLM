from pyspark.sql import functions as F

result = df_orders.withColumn("discount_filled", F.coalesce("discount", F.lit(0)))
