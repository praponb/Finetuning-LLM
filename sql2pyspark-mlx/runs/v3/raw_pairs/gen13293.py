from pyspark.sql import functions as F

result = df_suppliers.agg(F.sort_array(F.collect_set("country")).alias("country_list"))
