from pyspark.sql import functions as F

result = df_stores.select("id", (F.col("sqft") % 2).alias("is_odd"))
