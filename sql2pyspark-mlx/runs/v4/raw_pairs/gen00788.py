from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") <= 226)
    .withColumn("event_time_period", F.date_trunc("month", "event_time"))
    .withColumn("page_prefix", F.substring("page", 1, 4))
    .orderBy("duration_sec", "id")
    .limit(5)
)
