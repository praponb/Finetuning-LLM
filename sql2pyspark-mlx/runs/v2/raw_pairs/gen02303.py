from pyspark.sql import functions as F

result = df_employees.withColumn("full_name_prefix", F.substring("full_name", 1, 3))
