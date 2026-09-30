from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") < 257) | (F.col("event_time") < "2024-06-11"))
    .select("event_time", F.length("page").alias("page_length"))
    .orderBy("event_time")
)
