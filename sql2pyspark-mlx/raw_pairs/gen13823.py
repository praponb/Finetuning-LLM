from pyspark.sql import functions as F

result = df_accounts.agg(F.max("balance").alias("max_balance"))
