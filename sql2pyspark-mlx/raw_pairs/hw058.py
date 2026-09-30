from pyspark.sql import functions as F

result = df_transactions.select("id", F.when(F.col("is_flagged"), "Review").otherwise("OK").alias("review_status"))
