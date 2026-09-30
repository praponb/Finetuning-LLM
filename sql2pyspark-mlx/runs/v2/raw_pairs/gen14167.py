from pyspark.sql import functions as F

result = df_subscriptions.select("start_date", "monthly_fee")
