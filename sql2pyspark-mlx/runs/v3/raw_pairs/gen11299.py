from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~((F.col("event_time") >= "2023-12-01") & ((F.col("event_time") < "2023-03-01") | F.col("session_id").isin([35, 40]))))
    .select("event_time", F.ceil("duration_sec").alias("duration_sec_whole"))
)
