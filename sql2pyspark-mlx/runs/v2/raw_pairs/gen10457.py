from pyspark.sql import functions as F

result = df_web_events.select("event_time", F.trim("page").alias("clean_page"))
