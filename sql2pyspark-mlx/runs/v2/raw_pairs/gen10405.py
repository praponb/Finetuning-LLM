from pyspark.sql import functions as F

result = df_accounts.agg(F.sum("balance").alias("total_balance"))
