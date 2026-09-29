from pyspark.sql import functions as F

result = df_orders.filter((F.col("discount") < 0.25) & (F.col("status") == "RETURNED"))
