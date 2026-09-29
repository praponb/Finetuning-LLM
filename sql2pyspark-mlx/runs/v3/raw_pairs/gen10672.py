from pyspark.sql import functions as F

result = df_stores.agg(F.countDistinct("sqft").alias("unique_citys"))
