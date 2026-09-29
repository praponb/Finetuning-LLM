from pyspark.sql import functions as F

result = df_subscriptions.select("id", "monthly_fee", "customer_id", "end_date")
