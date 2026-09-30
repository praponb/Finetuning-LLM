from pyspark.sql import functions as F

result = df_orders.filter(F.col("status").isin(["CANCELLED", "RETURNED"]))
