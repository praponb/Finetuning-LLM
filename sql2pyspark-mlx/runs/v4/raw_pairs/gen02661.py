from pyspark.sql import functions as F

result = df_payments.select("id", "method", "order_id")
