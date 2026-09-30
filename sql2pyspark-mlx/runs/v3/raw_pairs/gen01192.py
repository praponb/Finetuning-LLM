from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(12, 16) & (F.col("duration_sec") == 388))
    .withColumn("duration_sec_str", F.col("duration_sec").cast("string"))
)
