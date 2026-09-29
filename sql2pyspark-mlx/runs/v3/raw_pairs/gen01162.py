from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") >= "2024-11-01") & F.hour("event_time").between(7, 15))
)
