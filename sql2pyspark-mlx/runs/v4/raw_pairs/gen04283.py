from pyspark.sql import functions as F

result = df_transactions.filter(F.hour("txn_time").between(6, 9))
