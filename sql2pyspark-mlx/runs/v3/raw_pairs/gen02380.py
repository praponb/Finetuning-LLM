from pyspark.sql import functions as F

result = df_subscriptions.select("id", "plan", "monthly_fee")
