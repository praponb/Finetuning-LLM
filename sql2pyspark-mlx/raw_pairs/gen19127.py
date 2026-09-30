from pyspark.sql import functions as F

result = df_web_events.withColumn("duration_sec_rounded", F.round("duration_sec", 0))
