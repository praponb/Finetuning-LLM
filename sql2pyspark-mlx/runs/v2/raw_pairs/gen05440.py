from pyspark.sql import functions as F

result = df_stores.select("id", "city", F.trim("city").alias("clean_city"))
