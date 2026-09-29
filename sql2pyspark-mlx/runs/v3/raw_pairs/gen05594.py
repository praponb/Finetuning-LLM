from pyspark.sql import functions as F

result = df_web_events.select("id", "page", F.lower("page").alias("page_lower"))
