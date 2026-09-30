from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(270, 440) & ((F.to_date("event_time") < "2023-09-22") | F.col("event_type").isin(["click", "purchase"])))
    .withColumn("event_time_dayofweek", F.dayofweek("event_time"))
    .withColumn("event_time_day", F.dayofmonth("event_time"))
)
