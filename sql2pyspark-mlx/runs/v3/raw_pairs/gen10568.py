from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(7, 10) & ((F.to_date("event_time") >= "2025-02-01") | F.col("session_id").isin([7, 22])))
    .select("event_time", F.hour("event_time").alias("event_time_hour"))
    .orderBy("event_time_hour")
)
