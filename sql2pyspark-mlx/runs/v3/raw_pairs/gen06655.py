from pyspark.sql import functions as F

result = df_payments.select("id", "paid_at", "method", "order_id")
