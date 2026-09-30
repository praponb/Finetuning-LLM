from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(7, 10) | F.hour("event_time").between(10, 17))
    .select("event_time", F.lower("page").alias("page_lower"))
)
