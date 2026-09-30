from pyspark.sql import functions as F

result = df_stores.withColumn("region_title", F.initcap(F.trim("region")))
