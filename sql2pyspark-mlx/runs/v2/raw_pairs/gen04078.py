from pyspark.sql import functions as F

result = df_payments.select("method", "paid_at", "order_id")
