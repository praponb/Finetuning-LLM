from pyspark.sql import functions as F

result = df_stores.select("opened_date", F.length("region").alias("region_length"))
