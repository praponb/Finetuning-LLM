from pyspark.sql import functions as F

result = df_accounts.agg(F.max("opened_date").alias("last_opened_date"))
