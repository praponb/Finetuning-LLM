from pyspark.sql import functions as F

result = df_subscriptions.select("id", "status", "monthly_fee", "plan")
