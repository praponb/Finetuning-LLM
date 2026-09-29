from pyspark.sql import functions as F

result = df_stores.filter(F.col("sqft") <= 12042)
