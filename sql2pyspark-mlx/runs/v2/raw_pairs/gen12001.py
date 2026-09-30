from pyspark.sql import functions as F

result = df_accounts.agg((F.max("balance") - F.min("balance")).alias("balance_range"))
