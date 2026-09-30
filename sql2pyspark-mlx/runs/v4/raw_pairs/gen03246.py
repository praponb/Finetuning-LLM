from pyspark.sql import functions as F

result = df_transactions.agg(F.max("amount").alias("max_amount"))
