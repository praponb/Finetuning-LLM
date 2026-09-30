from pyspark.sql import functions as F

result = df_transactions.select("merchant", "txn_time", "account_id")
