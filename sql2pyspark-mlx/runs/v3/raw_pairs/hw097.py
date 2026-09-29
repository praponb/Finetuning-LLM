from pyspark.sql import functions as F

result = df_stores.rollup("region", "city").agg(F.sum("sqft").alias("total_sqft"))
