from pyspark.sql import functions as F

result = df_payments.filter((F.col("amount") >= 1530) & F.col("order_id").isin([2, 14, 31]))
