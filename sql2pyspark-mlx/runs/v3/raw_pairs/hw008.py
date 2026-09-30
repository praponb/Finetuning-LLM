from pyspark.sql import functions as F

result = df_transactions.filter(F.col("amount").between(50, 2000))
