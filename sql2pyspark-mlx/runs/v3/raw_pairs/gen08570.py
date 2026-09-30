from pyspark.sql import functions as F

result = df_subscriptions.select("id", "start_date", "status")
