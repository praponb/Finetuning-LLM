from pyspark.sql import functions as F

result = df_payments.select("id", "status", "amount", F.trim("method").alias("clean_method"))
