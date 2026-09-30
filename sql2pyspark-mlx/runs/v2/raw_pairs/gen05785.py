from pyspark.sql import functions as F

result = df_employees.withColumn("full_name_title", F.initcap(F.trim("full_name")))
