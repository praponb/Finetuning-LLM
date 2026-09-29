from pyspark.sql import functions as F

result = df_stores.agg(F.avg("sqft").alias("avg_sqft"))
