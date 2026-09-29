from pyspark.sql import functions as F

result = df_shipments.agg(F.sum("cost").alias("total_cost"))
