from pyspark.sql import functions as F

result = df_payments.filter(F.col("order_id").isin([2, 11, 27, 30]))
