from pyspark.sql import functions as F

result = df_web_events.filter(F.col("session_id").isin([2, 15]))
