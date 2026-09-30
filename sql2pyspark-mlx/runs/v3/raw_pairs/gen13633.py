from pyspark.sql import functions as F

result = df_subscriptions.agg(F.sort_array(F.collect_set("status")).alias("status_list"))
