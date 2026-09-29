from pyspark.sql import functions as F

result = df_subscriptions.select("id", "status", "customer_id", "start_date")
