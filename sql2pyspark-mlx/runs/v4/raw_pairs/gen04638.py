from pyspark.sql import functions as F

result = df_accounts.select("id", "account_type", "opened_date", "balance")
