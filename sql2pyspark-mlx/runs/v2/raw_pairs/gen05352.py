from pyspark.sql import functions as F

result = df_web_events.agg(F.max("duration_sec").alias("max_duration_sec"))
