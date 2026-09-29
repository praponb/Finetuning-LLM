from pyspark.sql import functions as F

result = df_web_events.filter(F.col("duration_sec").isNull())
