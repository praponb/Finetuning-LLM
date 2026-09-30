from pyspark.sql import functions as F

result = df_web_events.withColumn("event_time_label", F.date_format("event_time", "yyyy-MM"))
