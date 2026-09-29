from pyspark.sql import functions as F

result = (
    df_web_events
    .withColumn("duration_sec_whole", F.ceil("duration_sec"))
    .withColumn("event_time_date", F.to_date("event_time"))
    .orderBy(F.col("event_time").desc())
)
