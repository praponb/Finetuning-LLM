from pyspark.sql import functions as F

result = df_accounts.select("customer_id", "account_type", "status")
