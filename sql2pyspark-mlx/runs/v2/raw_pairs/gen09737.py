from pyspark.sql import functions as F

result = df_stores.withColumn("store_name_title", F.initcap(F.trim("store_name")))
