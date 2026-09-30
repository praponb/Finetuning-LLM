from pyspark.sql import functions as F

result = df_suppliers.select("id", "country", F.trim("city").alias("clean_city"))
