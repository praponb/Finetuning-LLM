from pyspark.sql import functions as F

result = df_accounts.withColumn("status_title", F.initcap(F.trim("status")))
