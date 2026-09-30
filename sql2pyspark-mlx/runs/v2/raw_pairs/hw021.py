from pyspark.sql import functions as F

result = df_web_events.agg(F.count("*").alias("total_rows"))
