from pyspark.sql import functions as F

result = df_payments.select("id", "order_id", "paid_at")
