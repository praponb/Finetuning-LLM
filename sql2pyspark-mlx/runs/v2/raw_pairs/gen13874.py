from pyspark.sql import functions as F

result = df_customers.withColumn("city_title", F.initcap(F.trim("city")))
