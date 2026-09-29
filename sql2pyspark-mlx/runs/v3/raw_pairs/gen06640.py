from pyspark.sql import functions as F

result = df_stores.filter((F.col("city") == "Tokyo") & (F.col("sqft") <= 9198))
