from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(
        F.count("*").alias("cnt"),
        F.min("event_time").alias("first_event_time"),
    )
    .filter(F.col("cnt") < 6)
    .select("event_type", "cnt", "first_event_time")
)
