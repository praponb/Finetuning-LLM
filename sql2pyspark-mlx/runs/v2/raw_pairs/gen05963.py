from pyspark.sql import functions as F

result = df_employees.select("email", F.substring("level", 1, 3).alias("level_prefix"))
