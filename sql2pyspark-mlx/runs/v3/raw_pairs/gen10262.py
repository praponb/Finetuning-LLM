from pyspark.sql import functions as F

result = df_payments.select("amount", "order_id")
