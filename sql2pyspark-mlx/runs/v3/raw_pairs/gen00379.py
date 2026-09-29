from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_time") < "2023-11-18")
    .withColumn("event_time_label", F.date_format("event_time", "yyyy-MM"))
    .withColumn("event_time_day", F.dayofmonth("event_time"))
)
