from pyspark.sql import functions as F

result = df_web_events.agg(F.min("duration_sec").alias("min_duration_sec"))
