from pyspark.sql import functions as F

result = df_orders.filter((F.col("status") == "COMPLETED") & (F.col("amount") >= 100))
