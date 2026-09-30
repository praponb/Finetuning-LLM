from pyspark.sql import functions as F

result = df_accounts.select("id", "customer_id", F.upper("status").alias("status_upper"))
