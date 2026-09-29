from pyspark.sql import functions as F

result = df_transactions.select("merchant", "amount", "txn_time")
