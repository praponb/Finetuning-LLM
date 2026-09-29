from pyspark.sql import functions as F

result = df_accounts.select("id", F.date_format("opened_date", "yyyy/MM").alias("period"))
