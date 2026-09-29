from pyspark.sql import functions as F

result = df_payments.select("id", "paid_at", F.ceil("amount").alias("amount_whole"))
