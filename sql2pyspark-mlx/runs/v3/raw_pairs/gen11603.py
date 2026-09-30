from pyspark.sql import functions as F

result = df_payments.select("id", "order_id", F.hour("paid_at").alias("paid_at_hour"))
