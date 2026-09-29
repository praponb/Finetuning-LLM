from pyspark.sql import functions as F

result = df_subscriptions.withColumn("status_prefix", F.substring("status", 1, 4))
