from pyspark.sql import functions as F

result = df_stores.select("opened_date", "region", F.length("city").alias("city_length"))
