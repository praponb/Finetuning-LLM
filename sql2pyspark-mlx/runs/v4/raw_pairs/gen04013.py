from pyspark.sql import functions as F

result = df_subscriptions.select("id", "customer_id", "monthly_fee", "plan")
