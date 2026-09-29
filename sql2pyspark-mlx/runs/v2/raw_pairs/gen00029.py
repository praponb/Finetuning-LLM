from pyspark.sql import functions as F

result = df_employees.withColumn("salary_whole", F.ceil("salary"))
