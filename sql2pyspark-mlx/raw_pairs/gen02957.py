from pyspark.sql import functions as F

result = df_sessions.filter(F.col("customer_id").isin([3, 18, 22, 38]))
