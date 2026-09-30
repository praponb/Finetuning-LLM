from pyspark.sql import functions as F

result = df_transactions.filter(F.col("txn_time") < "2024-10-16")
