from pyspark.sql import functions as F

result = df_shipments.agg(F.sort_array(F.collect_set("carrier")).alias("carrier_list"))
