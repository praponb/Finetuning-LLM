from pyspark.sql import functions as F

result = df_payments.select("id", F.to_date("paid_at").alias("paid_date"))
