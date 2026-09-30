from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~((F.col("event_time") >= "2023-06-01") & F.col("session_id").isin([28, 34, 37])))
    .orderBy(F.col("event_time").desc())
)
