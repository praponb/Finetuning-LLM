from pyspark.sql import functions as F

result = df_customers.select("id", F.length(F.trim("name")).alias("name_len"))
