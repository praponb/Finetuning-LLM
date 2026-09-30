from pyspark.sql import functions as F

result = df_payments.select("method", "amount", "order_id")
