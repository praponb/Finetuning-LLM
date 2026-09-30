from pyspark.sql import functions as F

result = df_customers.filter(F.col("segment").isin(["Consumer", "SMB", "Corporate"]))
