from pyspark.sql import functions as F

result = df_accounts.filter(F.col("opened_date") >= "2023-08-01")
