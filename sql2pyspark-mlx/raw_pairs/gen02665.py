from pyspark.sql import functions as F

result = df_accounts.select("id", "status", "balance", "opened_date")
