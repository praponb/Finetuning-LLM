from pyspark.sql import functions as F

result = df_payments.select("id", "method", "status", "paid_at")
