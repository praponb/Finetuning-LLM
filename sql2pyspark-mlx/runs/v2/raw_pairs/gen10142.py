from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(10, 16))
    .groupBy(F.quarter("event_time").alias("event_quarter"))
    .pivot("page", ["/search", "/home"])
    .agg(F.max("duration_sec"))
    .orderBy("event_quarter")
)
