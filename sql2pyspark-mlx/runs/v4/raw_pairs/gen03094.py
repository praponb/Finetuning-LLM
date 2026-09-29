from pyspark.sql import functions as F

result = df_subscriptions.select("customer_id", "monthly_fee", "end_date")
