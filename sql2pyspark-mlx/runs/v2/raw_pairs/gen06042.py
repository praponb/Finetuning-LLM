from pyspark.sql import functions as F

result = df_shipments.agg(F.round(F.stddev("cost"), 2).alias("stddev_cost"))
