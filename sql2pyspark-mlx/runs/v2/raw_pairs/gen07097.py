from pyspark.sql import functions as F

result = df_stores.filter(F.col("sqft").between(7817, 9646) | (F.col("city") == "Berlin"))
