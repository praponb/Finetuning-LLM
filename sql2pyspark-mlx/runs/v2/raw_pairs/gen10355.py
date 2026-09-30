from pyspark.sql import functions as F

result = df_transactions.select("id", "amount", "is_flagged", "txn_time")
