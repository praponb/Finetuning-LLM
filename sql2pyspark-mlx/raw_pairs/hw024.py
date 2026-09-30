from pyspark.sql import functions as F

result = df_customers.groupBy("country").agg(F.avg("age").alias("avg_age"))
