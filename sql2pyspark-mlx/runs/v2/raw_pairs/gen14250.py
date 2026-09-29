from pyspark.sql import functions as F

result = df_shipments.select("shipped_date", F.floor("cost").alias("cost_whole"))
