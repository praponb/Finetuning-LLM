from pyspark.sql import functions as F

result = df_subscriptions.select("status", "customer_id", "start_date")
