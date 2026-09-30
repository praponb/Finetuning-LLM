from pyspark.sql import functions as F

result = df_transactions.select("account_id")
