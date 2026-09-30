from pyspark.sql import functions as F

result = df_payments.agg(F.sort_array(F.collect_set("method")).alias("method_list"))
