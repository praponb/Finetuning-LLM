from pyspark.sql import functions as F

result = df_transactions.filter(~F.col("merchant").isin(["Amazon", "Grab"]))
